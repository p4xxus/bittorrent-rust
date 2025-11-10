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

        if let Err(peer_error) = self.run().await {
            error!("Peer quit: {peer_error}");
            emit_peer_event(
                crate::events::PeerEvent::Disconnected(Arc::new(peer_error), connection_type),
                info_hash,
            );
        }
    }

    async fn run(mut self) -> Result<(), PeerError> {
        // TODO: do choking
        self.state.0.am_choking.store(false, Ordering::Relaxed);

        let mut receiver_stream = mem::take(&mut self.receiver_stream)
            .expect("The receiver stream is initialized after creation of the peer.");

        // for the inital handshake, look in conn.rs
        self.send_extended_handshake().await?;

        // this message is essentially which kick-starts the loop
        self.send_peer_manager(ReqMessage::WhatDoWeHave).await?;
        loop {
            if let Some(message) = receiver_stream.next().await {
                // debug!("INCOMING: {message:?}");
                match message {
                    Msg::Manager(peer_msg) => match peer_msg {
                        ResMessage::FinishedFile => {
                            if !self.state.0.peer_interested.load(Ordering::Relaxed) {
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
                            self.queue
                                .to_send
                                .extend_from_slice(&req_piece_payload_msgs);

                            // TODO: keep interested state up-to-date
                            // if self.queue.to_send.is_empty() {
                            //     self.set_interested(false).await?;
                            // } else {
                            //     self.set_interested(true).await?;
                            // };
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
                                }
                            } else {
                                self.set_interested(true).await?;
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
                            self.request_block_queue().await?;
                        }
                    },
                    Msg::Data(message) => match message {
                        PeerMessage::Choke(_no_payload) => {
                            self.state.0.peer_choking.store(true, Ordering::Relaxed)
                        }
                        PeerMessage::Unchoke(_no_payload) => {
                            // dbg!("peer unchokes");
                            self.state.0.peer_choking.store(false, Ordering::Relaxed);
                        }
                        PeerMessage::Interested(_no_payload) => {
                            self.state.0.peer_interested.store(true, Ordering::Relaxed);
                            // TODO: choking
                            self.send_peer(PeerMessage::Unchoke(NoPayload)).await?;
                        }
                        PeerMessage::NotInterested(_no_payload) => {
                            self.state.0.peer_interested.store(false, Ordering::Relaxed);
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
                        PeerMessage::Cancel(_request_piece_payload) => todo!(),
                        PeerMessage::KeepAlive(_no_payload) => {}
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
        if self.queue.to_send.is_empty() && self.state.0.am_interested.load(Ordering::Relaxed) {
            self.send_peer_manager(ReqMessage::NeedBlockQueue).await
        } else {
            Ok(())
        }
    }
}
