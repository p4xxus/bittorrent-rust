use riptorrent::events::ApplicationEvent;
use tokio::sync::broadcast;

use crate::model::{Message, Model};

pub fn handle_client_event(
    _: &Model,
    rx: &mut broadcast::Receiver<ApplicationEvent>,
) -> Option<Message> {
    if let Ok(event) = rx.try_recv() {
        Some(Message::ApplicationEvent(event))
    } else {
        None
    }
}

pub(super) fn update_from_application_event(model: &mut Model, event: ApplicationEvent) {
    match event {
        ApplicationEvent::Peer(peer_event, info_hash) => match peer_event {
            riptorrent::events::PeerEvent::NewConnection(connection_type) => {
                if let Some(torrent) = model.torrents.get_mut(&info_hash) {
                    match connection_type {
                        riptorrent::events::ConnectionType::Inbound => {
                            torrent.peer_connections.increase_inbound(1)
                        }
                        riptorrent::events::ConnectionType::Outbound => {
                            torrent.peer_connections.increase_outbound(1)
                        }
                    }
                }
            }
            riptorrent::events::PeerEvent::Disconnected(peer_error, connection_type) => {
                if let Some(torrent) = model.torrents.get_mut(&info_hash) {
                    match connection_type {
                        riptorrent::events::ConnectionType::Inbound => {
                            torrent.peer_connections.increase_inbound(1)
                        }
                        riptorrent::events::ConnectionType::Outbound => {
                            torrent.peer_connections.increase_outbound(1)
                        }
                    }
                }
            }
        },

        ApplicationEvent::Torrent(torrent_event, info_hash) => match torrent_event {
            riptorrent::events::TorrentEvent::NewDownload => (),
            riptorrent::events::TorrentEvent::GotFileInfo(file_info) => {
                model.torrents.insert(file_info.info_hash, file_info.into());
            }
            riptorrent::events::TorrentEvent::GotPiece(piece_i) => {
                if let Some(torrent) = model.torrents.get_mut(&info_hash)
                    && let Some(we_have) = torrent.bitfield.get_mut(piece_i as usize)
                {
                    *we_have = true
                }
            }
            riptorrent::events::TorrentEvent::DownloadCanceled(peer_manager_error) => (),
            riptorrent::events::TorrentEvent::Finished => (),
        },
    }
}
