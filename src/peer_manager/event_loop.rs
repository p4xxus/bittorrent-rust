use std::path::PathBuf;

use crate::{
    PeerManager, Torrent,
    extensions::{ExtensionMessage, ExtensionType},
    magnet_links::metadata_piece_manager::MetadataPieceManager,
    messages::payloads::BitfieldPayload,
    peer::DEFAULT_MAX_REQUESTS,
    peer_manager::{
        PeerId, PieceManager, ReqMessage, ResMessage, TorrentState, error::PeerManagerError,
    },
    torrent::Metainfo,
};

impl PeerManager {
    pub(crate) async fn run(mut self) -> Result<(), PeerManagerError> {
        self.req_tracker()
            .await
            .map_err(|e| PeerManagerError::Other(Box::new(e)))?;
        while let Some(peer_msg) = self.rx.recv().await {
            match peer_msg.msg {
                ReqMessage::NewConnection(peer_conn) => {
                    self.peers.insert(peer_msg.peer_id, peer_conn);
                    self.piece_selector.add_peer(peer_msg.peer_id);

                    // TODO: here we might want to check interested and also make the peer set its interested flag if he receives this
                    self.send_peer(peer_msg.peer_id, ResMessage::StartDownload)
                        .await;
                }
                ReqMessage::GotBlock(block) => {
                    if let TorrentState::Downloading {
                        metainfo,
                        piece_manager,
                    } = &mut self.torrent_state
                        && let Some(piece_index) = piece_manager
                            .write_block(&mut self.piece_selector, block, metainfo, &self.db_conn) // TODO: maybe put the db_conn inside the piece_manager itself, bc rn we Arc::clone() it for every piece finished
                            .await?
                    {
                        let msg = ResMessage::FinishedPiece(piece_index);
                        eprintln!("Finished piece number {piece_index}.");
                        if self.is_finished() {
                            self.transition_seeding();
                            self.broadcast_peers(ResMessage::FinishedFile).await;
                        }
                        self.broadcast_peers(msg).await;
                    }
                }
                ReqMessage::NeedBlock(block) => {
                    if let TorrentState::Downloading {
                        metainfo,
                        piece_manager,
                    } = &self.torrent_state
                    {
                        let block =
                            piece_manager.get_block(&mut self.piece_selector, block, metainfo);
                        let msg = ResMessage::Block(block);
                        self.send_peer(peer_msg.peer_id, msg).await;
                    }
                }
                ReqMessage::NeedBlockQueue => {
                    let max_req = self.get_peers_max_req(&peer_msg.peer_id);
                    if let TorrentState::Downloading {
                        metainfo,
                        piece_manager,
                    } = &mut self.torrent_state
                    {
                        if let Some(blocks) = piece_manager.prepare_next_blocks(
                            &mut self.piece_selector,
                            max_req as usize,
                            &peer_msg.peer_id,
                            metainfo,
                        ) {
                            let msg = ResMessage::NewBlockQueue(blocks);
                            self.send_peer(peer_msg.peer_id, msg).await;
                        }
                    } else if let TorrentState::WaitingForMetadata {
                        file_path: _,
                        metadata_piece_manager,
                    } = &mut self.torrent_state
                    {
                        let msg = get_metadata_queue(metadata_piece_manager)?;
                        if let Some(msg) = msg {
                            self.send_peer(peer_msg.peer_id, msg).await;
                        }
                    }
                }
                ReqMessage::WhatDoWeHave => {
                    let have = self.piece_selector.get_have();
                    let have = if have.is_empty() { None } else { Some(have) };
                    let msg =
                        ResMessage::WeHave(have.map(|have| BitfieldPayload::new(have.clone())));
                    self.send_peer(peer_msg.peer_id, msg).await;
                }
                ReqMessage::Extension(extension_message) => {
                    if let TorrentState::WaitingForMetadata {
                        file_path,
                        metadata_piece_manager,
                    } = &mut self.torrent_state
                    {
                        match extension_message {
                            ExtensionMessage::ReceivedMetadataPiece { piece_index, data } => {
                                println!("Received metadata Block with index {piece_index}");
                                metadata_piece_manager.add_block(piece_index, data);

                                if metadata_piece_manager.check_finished() {
                                    let file_path = file_path.clone();
                                    let metainfo = metadata_piece_manager
                                    .get_metadata()
                                        .expect("This shouldn't fail since we checked that the hashes match.");
                                    self.transition_downloading(metainfo, file_path).await?;
                                }
                            }
                            ExtensionMessage::GotMetadataLength(length) => {
                                metadata_piece_manager.set_len(length);
                            }
                        }
                    }
                }
                ReqMessage::PeerDisconnected(peer_id) => {
                    self.peers.remove(&*peer_id);
                    self.piece_selector.remove_peer(&*peer_id);
                }
                ReqMessage::PeerBitfield(bitfield) => {
                    self.piece_selector
                        .add_peer_bitfield(&peer_msg.peer_id, bitfield.get_pieces());
                }
                ReqMessage::PeerHas(have_payload) => {
                    self.piece_selector
                        .update_from_peer_have(&peer_msg.peer_id, have_payload.piece_index);
                }
            }
        }

        Ok(())
    }

    fn transition_seeding(&mut self) {
        todo!("we're done downloading")
    }

    async fn transition_downloading(
        &mut self,
        metainfo: Metainfo,
        file_path: Option<PathBuf>,
    ) -> Result<(), PeerManagerError> {
        let file_path = file_path.unwrap_or(metainfo.name.clone().into());

        let torrent = Torrent {
            announce: self
                .announce_urls
                .first()
                .expect("If there's none, the parsing would have failed long ago.")
                .clone(),
            info: metainfo,
        };
        let db_entry = self.db_conn.set_entry(file_path.clone(), torrent).await?;

        let piece_manager =
            PieceManager::build(file_path, db_entry.torrent_info.info_hash().as_hex(), false)?;
        self.piece_selector
            .update_from_self_have(db_entry.bitfield.to_vec());
        self.torrent_state = TorrentState::Downloading {
            metainfo: db_entry.torrent_info,
            piece_manager,
        };

        self.broadcast_peers(ResMessage::StartDownload).await;
        eprintln!("Finished downloading the metainfo.");

        Ok(())
    }

    async fn send_peer(&mut self, peer_id: PeerId, msg: ResMessage) {
        if let Some(peer) = self.peers.get_mut(&peer_id)
            && peer.send(msg, peer_id).await.is_ok()
        {
        } else {
            self.peers.remove(&peer_id);
        }
    }

    async fn broadcast_peers(&mut self, msg: ResMessage) {
        for (&peer_id, conn) in self.peers.iter() {
            let _ = conn.send(msg.clone(), peer_id).await;
            // if we fail to send here, it doesn't really matter tbh
        }
    }

    fn get_peers_max_req(&self, id: &PeerId) -> u32 {
        self.peers
            .get(id)
            .map(|p| {
                p.identifier
                    .0
                    .max_req
                    .load(std::sync::atomic::Ordering::Relaxed)
                    - 10 // probably just as a margin
            })
            .unwrap_or(DEFAULT_MAX_REQUESTS)
    }

    fn is_finished(&self) -> bool {
        self.piece_selector.get_have().iter().all(|b| *b)
    }
}

/// helper function that get's the new blocks to be added and creates a message of it
/// it's not really a queue, rather just one message
fn get_metadata_queue(
    metadata_piece_manager: &mut MetadataPieceManager,
) -> Result<Option<ResMessage>, PeerManagerError> {
    let new_data = metadata_piece_manager
        .get_block_req_data()
        .map_err(|e| PeerManagerError::Other(Box::new(e)))?;

    if let Some(data) = new_data {
        Ok(Some(ResMessage::ExtensionData((
            ExtensionType::Metadata,
            data,
        ))))
    } else {
        Ok(None)
    }
}
