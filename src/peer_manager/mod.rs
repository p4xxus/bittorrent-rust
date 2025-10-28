//! a peer announces to us that he exists via the mpsc
//! We create peer with our current have bitfield which he can send to new connections and we send
use std::{
    collections::HashMap, fmt::Debug, net::SocketAddrV4, path::PathBuf, sync::Arc, time::Duration,
};

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::{
    Peer, TrackerRequest,
    client::PEER_ID,
    database::{DBEntry, SurrealDbConn},
    extensions::{
        ExtensionMessage, ExtensionType,
        magnet_links::{MagnetLink, metadata_piece_manager::MetadataPieceManager},
    },
    messages::payloads::{BitfieldPayload, HavePayload, RequestPiecePayload, ResponsePiecePayload},
    peer::conn::PeerState,
    peer_manager::{
        error::PeerManagerError,
        piece_manager::{PieceManager, piece_selector::PieceSelector},
    },
    torrent::{InfoHash, Metainfo},
    tracker::TrackerRequestError,
};

pub mod error;
mod event_loop;
mod piece_manager;

const CHANNEL_SIZE: usize = 64;
type PeerId = [u8; 20];

/// how many pieces are in the queue at max
pub(crate) const MAX_PIECES_IN_PARALLEL: usize = 20;
/// after what Duration to re-request blocks
const TIMEOUT_FOR_REQ: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct PeerManager {
    torrent_state: TorrentState,
    db_conn: Arc<SurrealDbConn>,
    tx: mpsc::Sender<ReqMsgFromPeer>,
    rx: mpsc::Receiver<ReqMsgFromPeer>,
    announce_urls: Vec<url::Url>,
    peers: HashMap<PeerId, PeerConn>,
    piece_selector: PieceSelector,
}

#[derive(Debug)]
enum TorrentState {
    // We are waiting for metadata.
    WaitingForMetadata {
        file_path: Option<PathBuf>,
        metadata_piece_manager: MetadataPieceManager, // A helper to track downloaded metadata pieces
    },
    // We have the metadata and can download the actual files.
    Downloading {
        metainfo: Metainfo,
        piece_manager: PieceManager,
    },
    // Optional: A seeding state
    Seeding {
        metainfo: Metainfo,
        // ... state relevant to seeding
    },
}

/// from peer
#[derive(Debug, Clone, PartialEq)]
pub enum ReqMessage {
    NewConnection(PeerConn),
    NeedBlockQueue,
    GotBlock(ResponsePiecePayload),
    NeedBlock(RequestPiecePayload),
    // TODO: maybe remove this
    WhatDoWeHave,
    Extension(ExtensionMessage),
    PeerDisconnected(InfoHash),
    PeerHas(HavePayload),
    PeerBitfield(BitfieldPayload),
}

/// A message sent by a local peer to this Manager
pub struct ReqMsgFromPeer {
    pub(crate) peer_id: [u8; 20],
    pub(crate) msg: ReqMessage,
}

// TODO Next-up:
//  - rarest-first-piece-selection
//  - choking: 4 active downloaders

/// A message sent to the peer
#[derive(Debug, Clone, PartialEq)]
pub enum ResMessage {
    /// indication to the peer to start the download loop
    StartDownload,
    NewBlockQueue(Vec<RequestPiecePayload>),
    Block(Option<ResponsePiecePayload>),
    WeHave(Option<BitfieldPayload>),
    FinishedPiece(u32),
    FinishedFile,
    /// Data that is passed to BasicExtensionPayload.
    /// The peer has to 'add' the extended_msg_id itself since it is peer-dependent
    ExtensionData((ExtensionType, Bytes)),
}

#[derive(Debug, Clone)]
pub struct PeerConn {
    pub(super) sender: mpsc::Sender<ResMessage>,
    pub(super) identifier: PeerState,
}

impl PartialEq for PeerConn {
    fn eq(&self, other: &Self) -> bool {
        self.identifier.0.peer_id == other.identifier.0.peer_id
    }
}

impl PeerConn {
    async fn send(&self, msg: ResMessage, peer_id: [u8; 20]) -> Result<(), PeerManagerError> {
        self.sender
            .send(msg)
            .await
            .map_err(|error| PeerManagerError::SendError {
                peer_id,
                error,
                msg: "sending a message".to_string(),
            })
    }
}

#[derive(Clone)]
pub(crate) struct PieceState {
    blocks: Vec<BlockState>,
    piece_i: u32,
    buf: BytesMut,
}

impl Debug for PieceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PieceState")
            .field("blocks", &self.blocks)
            .field("piece_i", &self.piece_i)
            .finish()
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub(crate) enum BlockState {
    Finished,
    InProcess(std::time::Instant),
    None,
}

impl BlockState {
    pub(self) fn is_finished(&self) -> bool {
        *self == BlockState::Finished
    }
    pub(self) fn is_not_requested(&self) -> bool {
        *self == BlockState::None
            || matches!(self, BlockState::InProcess(i) if i.elapsed() >= TIMEOUT_FOR_REQ)
    }
}

impl PeerManager {
    pub(crate) fn init_from_entry(
        info_hash_hex: String,
        db_conn: Arc<SurrealDbConn>,
        file_entry: DBEntry,
    ) -> Result<Self, PeerManagerError> {
        let piece_manager =
            PieceManager::build(file_entry.file.to_path_buf(), info_hash_hex, true)?;
        let torrent_state = TorrentState::Downloading {
            metainfo: file_entry.torrent_info,
            piece_manager,
        };

        Ok(Self::new(
            torrent_state,
            db_conn,
            vec![file_entry.announce],
            PieceSelector::new(file_entry.bitfield.to_vec()),
        ))
    }

    pub(crate) fn init_from_magnet(
        magnet_link: MagnetLink,
        db_conn: Arc<SurrealDbConn>,
        file_path: Option<PathBuf>,
    ) -> Self {
        let torrent_state = TorrentState::WaitingForMetadata {
            file_path,
            metadata_piece_manager: MetadataPieceManager::new(magnet_link.info_hash),
        };
        Self::new(
            torrent_state,
            db_conn,
            magnet_link.get_announce_urls(),
            PieceSelector::new(vec![]),
        )
    }

    fn new(
        torrent_state: TorrentState,
        db_conn: Arc<SurrealDbConn>,
        announce_urls: Vec<url::Url>,
        piece_selector: PieceSelector,
    ) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        Self {
            torrent_state,
            rx,
            tx,
            db_conn,
            announce_urls,
            peers: HashMap::new(),
            piece_selector,
        }
    }

    async fn req_tracker(&self) -> Result<(), TrackerRequestError> {
        let file_length = match &self.torrent_state {
            TorrentState::WaitingForMetadata { file_path, .. } => 999, // this is just an arbitrary value really
            TorrentState::Downloading { metainfo, .. } => metainfo.get_length(),
            TorrentState::Seeding { metainfo } => metainfo.get_length(),
        };
        let info_hash = self.get_info_hash();

        // TODO: port
        let tracker = TrackerRequest::new(&info_hash, PEER_ID, 6882, file_length);
        let res = tracker.get_response(self.announce_urls.clone()).await?;

        self.add_peers_to_manager(res.peers.0).await;
        Ok(())
    }

    async fn add_peers_to_manager(&self, addresses: impl IntoIterator<Item = SocketAddrV4>) {
        let info_hash = self.get_info_hash();
        for addr in addresses {
            let peer_manager_tx = self.tx.clone();
            tokio::spawn(async move {
                let peer = Peer::connect_from_addr(addr, info_hash, *PEER_ID, peer_manager_tx)
                    .await
                    .expect("Failed to connect to peer.");
                peer.run().await.unwrap();
            });
        }
    }

    // TODO: we might want to save the InfoHash in the PeerManager itself
    fn get_info_hash(&self) -> InfoHash {
        match &self.torrent_state {
            TorrentState::WaitingForMetadata {
                metadata_piece_manager,
                ..
            } => metadata_piece_manager.get_info_hash(),
            TorrentState::Downloading { metainfo, .. } => metainfo.info_hash(),
            TorrentState::Seeding { metainfo } => metainfo.info_hash(),
        }
    }
}
