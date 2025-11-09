//! Contains the enums (and helper functions) that represent our events emitted by the client.

use std::{
    fmt::Debug,
    sync::{Arc, OnceLock},
};

use tokio::sync::broadcast;

use crate::{
    database::FileInfo, peer::error::PeerError, peer_manager::error::PeerManagerError,
    torrent::InfoHash,
};

// TODO: do we even need multiple subscriber?
static EVENT_SENDER: OnceLock<broadcast::Sender<ApplicationEvent>> = OnceLock::new();

pub(super) fn get_receiver() -> broadcast::Receiver<ApplicationEvent> {
    EVENT_SENDER
        .get_or_init(|| broadcast::channel(32).0)
        .subscribe()
}

/// sends an event to the EVENT_SENDER for the application to receive it
pub(super) fn emit_event(event: ApplicationEvent) {
    if let Some(sender) = EVENT_SENDER.get()
        && let Err(error) = sender.send(event)
    {
        panic!("Error when trying to send to the sender: {error:?}.");
    };
}

#[derive(Clone)]
pub enum ApplicationEvent {
    Peer(PeerEvent, InfoHash),
    Torrent(TorrentEvent, InfoHash),
}

impl Debug for ApplicationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Peer(arg0, _) => f.debug_tuple("Peer").field(arg0).finish(),
            Self::Torrent(arg0, _) => f.debug_tuple("Torrent").field(arg0).finish(),
        }
    }
}

/// Events that happen for individual peers
#[derive(Clone, Debug)]
pub enum PeerEvent {
    NewConnectionInbound,
    NewConnectionOutbound,
    Disconnected(Arc<PeerError>),
}

/// Events that happen for individual torrents
#[derive(Clone, Debug)]
pub enum TorrentEvent {
    NewDownload,
    NoTrackerResponse,
    /// the naming might be confusing (why not TorrentInfo)
    /// but it's kina like the public view onto a DBEntry
    GotFileInfo(FileInfo),
    GotPiece(u32),
    DownloadCanceled(Arc<PeerManagerError>),
    Finished,
}
