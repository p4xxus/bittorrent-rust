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
fn emit_event(event: ApplicationEvent) {
    if let Some(sender) = EVENT_SENDER.get()
        && let Err(error) = sender.send(event)
    {
        panic!("Error when trying to send to the sender: {error:?}.");
    };
}

pub(crate) fn emit_peer_event(peer_event: PeerEvent, info_hash: InfoHash) {
    emit_event(ApplicationEvent::Peer(peer_event, info_hash));
}

pub(crate) fn emit_torrent_event(torrent_event: TorrentEvent, info_hash: InfoHash) {
    emit_event(crate::events::ApplicationEvent::Torrent(
        torrent_event,
        info_hash,
    ));
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
    NewConnection(ConnectionType),
    /// contains an optional error (if we're finished and the peer is dropped, there's none) and the connection-type the peer was
    Disconnected(Option<Arc<PeerError>>, ConnectionType),
}

#[derive(Clone, Debug)]
pub enum ConnectionType {
    Inbound,
    Outbound,
}

/// Events that happen for individual torrents
#[derive(Clone, Debug)]
pub enum TorrentEvent {
    /// both if a download is resumed and if it's started for the first time
    StartDownload,
    Paused,
    Finished,

    GotFileInfo(FileInfo),
    GotPiece(u32),
    DownloadCanceled(Arc<PeerManagerError>),
}
