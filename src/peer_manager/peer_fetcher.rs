use std::net::SocketAddrV4;
use tokio::sync::mpsc;

use crate::{
    Peer, TrackerRequest,
    client::PEER_ID,
    peer_manager::ReqMsgFromPeer,
    torrent::{AnnounceList, InfoHash},
    tracker::TrackerResponse,
};

#[derive(Debug)]
pub(super) struct PeerFetcher {
    tx: mpsc::Sender<ReqMsgFromPeer>,
    /// something like [ [ tracker1, tracker2 ], [ backup1 ] ]
    pub(super) announce_list: AnnounceList,
}

impl PeerFetcher {
    pub(super) fn new(tx: mpsc::Sender<ReqMsgFromPeer>, announce_list: AnnounceList) -> Self {
        Self { tx, announce_list }
    }
    pub(super) async fn add_peers_to_manager(
        &self,
        info_hash: InfoHash,
        addresses: impl IntoIterator<Item = SocketAddrV4>,
    ) {
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

    pub(super) async fn get_tracker_response<'a>(
        &mut self,
        tracker_request: TrackerRequest<'a>,
    ) -> Option<TrackerResponse> {
        while let Some(tier) = self.announce_list.0.iter_mut().next()
            && let Some((url_index_in_tier, tracker_response)) = tracker_request
                .get_first_response_in_list(tier.clone())
                .await
        {
            let url = tier.remove(url_index_in_tier);
            tier.insert(0, url);

            return Some(tracker_response);
        }

        None
    }
}
