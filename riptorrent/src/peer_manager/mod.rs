//! a peer announces to us that he exists via the mpsc
//! We create peer with our current have bitfield which he can send to new connections and we send
use std::{collections::HashMap, fmt::Debug, path::PathBuf, sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::{
    database::{DBEntry, SurrealDbConn},
    events::{TorrentEvent, emit_event},
    extensions::{
        ExtensionMessage, ExtensionType,
        magnet_links::{MagnetLink, metadata_piece_manager::MetadataPieceManager},
    },
    messages::payloads::{BitfieldPayload, HavePayload, RequestPiecePayload, ResponsePiecePayload},
    peer::conn::PeerState,
    peer_manager::{
        error::PeerManagerError,
        peer_fetcher::PeerFetcher,
        piece_manager::{PieceManager, piece_selector::PieceSelector},
    },
    torrent::{AnnounceList, InfoHash, Metainfo},
};

pub mod error;
mod event_loop;
pub(super) mod peer_fetcher;
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
    peer_fetcher: PeerFetcher,
    // again, this is an option since I need to construct first the tx with this struct and then the receiver stream
    rx: Option<mpsc::Receiver<ReqMsgFromPeer>>,
    peers: HashMap<PeerId, PeerConn>,
    piece_selector: PieceSelector,
    pub info_hash: InfoHash,
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
    // at some point we might want to make a different PieceManager that is optimized for seeding but for now this is sufficient
    Seeding {
        metainfo: Metainfo,
        piece_manager: PieceManager,
    },
    // I've found no other way to do this since when transitioning to seeding, I have to move the state but cannot directly swap/transmute it
    /// DO NOT EVER CONSTRUCT THIS OTHER THAN FOR TRANSITIONING
    None,
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
#[derive(Debug, Clone, PartialEq)]
pub struct ReqMsgFromPeer {
    pub(crate) peer_id: [u8; 20],
    pub(crate) msg: ReqMessage,
}
#[derive(Debug, PartialEq, Clone)]
enum PeerManagerReceiverStream {
    PeerMessage(ReqMsgFromPeer),
    SendTrackerUpdate,
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
    /// if it's none, it's empty (we don't know how many pieces yet)
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
        db_conn: Arc<SurrealDbConn>,
        file_entry: DBEntry,
    ) -> Result<Self, PeerManagerError> {
        let info_hash = file_entry.torrent_info.info_hash();
        let info_hash_hex = info_hash.as_hex();
        let piece_manager = PieceManager::build(file_entry.file.to_path_buf(), info_hash_hex)?;

        emit_torrent_event(
            crate::events::TorrentEvent::GotFileInfo(file_entry.clone().into()),
            info_hash,
        );

        let torrent_state = TorrentState::Downloading {
            metainfo: file_entry.torrent_info,
            piece_manager,
        };
        Ok(Self::new(
            torrent_state,
            db_conn,
            file_entry.announce_list,
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
            AnnounceList::from_single_tier_list(magnet_link.get_announce_urls()),
            PieceSelector::new(vec![]),
        )
    }

    fn new(
        torrent_state: TorrentState,
        db_conn: Arc<SurrealDbConn>,
        announce_list: AnnounceList,
        piece_selector: PieceSelector,
    ) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
        let peer_fetcher = PeerFetcher::new(tx, announce_list);

        let info_hash_self = match &torrent_state {
            TorrentState::WaitingForMetadata {
                metadata_piece_manager,
                ..
            } => metadata_piece_manager.get_info_hash(),
            TorrentState::Downloading { metainfo, .. } => metainfo.info_hash(),
            TorrentState::Seeding { metainfo, .. } => metainfo.info_hash(),
            TorrentState::None => unreachable!("This state is only used for transitioning states"),
        };

        Self {
            torrent_state,
            rx: Some(rx),
            peer_fetcher,
            db_conn,
            peers: HashMap::new(),
            piece_selector,
            info_hash: info_hash_self,
        }
    }

    pub fn get_sender(&self) -> mpsc::Sender<ReqMsgFromPeer> {
        self.peer_fetcher.tx.clone()
    }
}

pub(in crate::peer_manager) fn emit_torrent_event(
    torrent_event: TorrentEvent,
    info_hash: InfoHash,
) {
    emit_event(crate::events::ApplicationEvent::Torrent(
        torrent_event,
        info_hash,
    ));
}
