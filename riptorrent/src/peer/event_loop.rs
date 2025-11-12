use futures_util::StreamExt;
use std::{
    mem,
    sync::{Arc, atomic::Ordering},
};
use tracing::{error, instrument, trace};

use crate::{
    events::{ConnectionType, emit_peer_event},
    extensions::BasicExtensionPayload,
    messages::{
        PeerMessage,
        payloads::{HavePayload, NoPayload},
    },
    peer::{Msg, Peer, error::PeerError},
    peer_manager::{ReqMessage, ResMessage},
    torrent::InfoHash,
};

impl Peer {
    /// runs the peer manager in this thread and handles its errors
    #[instrument(name = "Peer", skip_all ,fields(%info_hash, connection_type))]
    pub async fn run_gracefully(self, info_hash: InfoHash, connection_type: ConnectionType) {
        trace!("New connection.");

        let optional_error = match self.run().await {
            Err(peer_error) => {
                error!("Peer quit: {peer_error}");
                Some(Arc::new(peer_error))
            }
            Ok(_) => None,
        };

        emit_peer_event(
            crate::events::PeerEvent::Disconnected(optional_error, connection_type),
            info_hash,
        );
    }

    async fn run(mut self) -> Result<(), PeerError> {
        // TODO: do choking
        self.state.0.am_choking.store(false, Ordering::Relaxed);

        let mut receiver_stream = mem::take(&mut self.receiver_stream)
            .expect("The receiver stream is initialized after creation of the peer.");

        // for the inital handshake, look in conn.rs
        self.send_extended_handshake().await?;

        // how this is loop initiated:
        // - in conn.rs we send a NewConnection
        // - the PeerManager receives this and sends StartDownload
        // - we receive the StartDownload and ask which pieces we have
        // - the peer manager sends us the bitfield and we determine whether we're interested or not
        //  - if yes: send interested, request queue from manager, wait to become unchoked then request the queue and such
        //  - if no: well, we don't really do anything
        loop {
            if let Some(message) = receiver_stream.next().await {
                // debug!("INCOMING: {message:?}");
                match message {
                    Msg::Manager(peer_msg) => match peer_msg {
                        ResMessage::FinishedFile => {
                            if !self.state.0.peer_interested.load(Ordering::Acquire) {
                                break Ok(());
                            }
                        }
                        ResMessage::FinishedPiece(piece_index) => {
                            // later TODO: implement have suppression
                            let have_payload = HavePayload { piece_index };
                            self.send_peer(PeerMessage::Have(have_payload)).await?;
                        }
                        ResMessage::NewBlockQueue(request_piece_payloads) => {
                            let req_piece_payload_msgs: Vec<PeerMessage> = request_piece_payloads
                                .into_iter()
                                .map(PeerMessage::Request)
                                .collect();
                            trace!(
                                "Block queue of size {} coming",
                                req_piece_payload_msgs.len()
                            );
                            self.queue
                                .to_send
                                .extend_from_slice(&req_piece_payload_msgs);
                        }
                        ResMessage::Block(response_piece_payload) => {
                            if let Some(payload) = response_piece_payload {
                                self.send_peer(PeerMessage::Piece(payload)).await?;
                            }
                            // if we don't have the piece, Ig we just ignore
                        }
                        ResMessage::WeHave(bitfield) => {
                            // later TODO: implement lazy bitfield?
                            // also we might make this cleaner
                            if let Some(bitfield) = bitfield {
                                if bitfield.is_finished() {
                                    self.set_interested(false).await?;
                                } else {
                                    self.set_interested(true).await?;
                                    self.send_peer(PeerMessage::Bitfield(bitfield)).await?;

                                    // if we resumed it after pausing, we might still have something left in the queue
                                    if self.queue.to_send.is_empty() {
                                        self.request_block_queue().await?;
                                    }
                                }
                            } else {
                                // we have nothing yet
                                self.set_interested(true).await?;
                                self.request_block_queue().await?;
                            };
                        }
                        ResMessage::ExtensionData((ext_type, data)) => {
                            let msg = {
                                let extensions = self.extensions.lock().unwrap();
                                if let Some(extensions) = extensions.as_ref()
                                    && let Some(extension_id) = extensions.iter().find_map(|d| {
                                        (d.1.get_ext_type() == ext_type).then_some(*d.0)
                                    })
                                {
                                    Some(PeerMessage::Extended(BasicExtensionPayload {
                                        extension_id,
                                        data,
                                    }))
                                } else {
                                    None
                                }
                            };
                            if let Some(msg) = msg {
                                self.queue.to_send.push(msg);
                            }
                        }
                        ResMessage::StartDownload => {
                            self.send_peer_manager(ReqMessage::WhatDoWeHave).await?;
                        }
                        ResMessage::CancelDownload => {
                            self.set_interested(false).await?;
                            break Ok(());
                        }
                        ResMessage::PauseDownload => {
                            self.set_interested(false).await?;
                        }
                    },
                    Msg::Data(message) => match message {
                        PeerMessage::Choke(_) => {
                            self.state.0.peer_choking.store(true, Ordering::Relaxed)
                        }
                        PeerMessage::Unchoke(_) => {
                            trace!("peer unchokes");
                            self.state.0.peer_choking.store(false, Ordering::Relaxed);
                        }
                        PeerMessage::Interested(_) => {
                            self.state.0.peer_interested.store(true, Ordering::Release);
                            // TODO: choking
                            self.send_peer(PeerMessage::Unchoke(NoPayload)).await?;
                        }
                        PeerMessage::NotInterested(_) => {
                            self.state.0.peer_interested.store(false, Ordering::Release);
                        }
                        PeerMessage::Have(have_payload) => {
                            self.send_peer_manager(ReqMessage::PeerHas(have_payload))
                                .await?;
                        }
                        PeerMessage::Bitfield(bitfield_payload) => {
                            self.send_peer_manager(ReqMessage::PeerBitfield(bitfield_payload))
                                .await?;
                            self.request_block_queue().await?;
                        }
                        PeerMessage::Request(request_piece_payload) => {
                            self.send_peer_manager(ReqMessage::NeedBlock(request_piece_payload))
                                .await?;
                        }
                        PeerMessage::Piece(response_piece_payload) => {
                            self.queue.have_sent -= 1;
                            self.send_peer_manager(ReqMessage::GotBlock(response_piece_payload))
                                .await?;
                        }
                        PeerMessage::Cancel(_) => todo!(),
                        PeerMessage::KeepAlive(_) => {}
                        PeerMessage::Extended(extension_payload) => {
                            self.on_extension_data(extension_payload).await?;
                        }
                    },
                    Msg::Timeout => {
                        self.send_peer(PeerMessage::KeepAlive(NoPayload)).await?;
                    }
                    Msg::CloseConnection(error) => break Err(PeerError::PeerDisconnected(error)),
                }

                // request next blocks
                if self.queue.have_sent == 0
                    && self.state.0.am_interested.load(Ordering::Relaxed)
                    && !self.state.0.peer_choking.load(Ordering::Relaxed)
                // I use relaxed here since Ig, we'll just go to the next iteration if this loads something invalid, nothing will happen really
                {
                    let queue_iter: Vec<_> = mem::take(&mut self.queue.to_send);
                    self.queue.have_sent = queue_iter.len();
                    for req in queue_iter.into_iter() {
                        self.send_peer(req).await?;
                    }
                    self.request_block_queue().await?;
                }
            }
        }?;

        self.receiver_stream = Some(receiver_stream);
        Ok(())
    }

    /// requests the queue if our current one is empty and if we're interested
    /// ### first, set the interested state
    async fn request_block_queue(&self) -> Result<(), PeerError> {
        if self.queue.to_send.is_empty() && self.state.0.am_interested.load(Ordering::Acquire) {
            self.send_peer_manager(ReqMessage::NeedBlockQueue).await
        } else {
            Ok(())
        }
    }
}
