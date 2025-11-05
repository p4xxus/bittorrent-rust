use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc;

use crate::{
    BLOCK_MAX, Peer, PeerManager, TrackerRequest,
    client::{ClientOptions, PEER_ID},
    peer_manager::ReqMsgFromPeer,
    torrent::{AnnounceList, InfoHash},
    tracker::TrackerResponse,
};

const DEFAULT_TRACKER_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug)]
pub(super) struct PeerFetcher {
    pub(super) tx: mpsc::Sender<ReqMsgFromPeer>,
    /// something like [ [ tracker1, tracker2 ], [ backup1 ] ]
    pub(super) announce_list: AnnounceList,
    tracker_timeout: Duration,
}

impl PeerFetcher {
    pub(in crate::peer_manager) fn get_tracker_req_interval(&self) -> Duration {
        self.tracker_timeout
    }

    pub(super) fn new(tx: mpsc::Sender<ReqMsgFromPeer>, announce_list: AnnounceList) -> Self {
        Self {
            tx,
            announce_list,
            tracker_timeout: DEFAULT_TRACKER_TIMEOUT,
        }
    }

    pub(super) async fn add_peers_to_manager(
        &self,
        info_hash: InfoHash,
        addresses: impl IntoIterator<Item = SocketAddr>,
    ) {
        for addr in addresses {
            println!("Connecting to {addr}");
            let peer_manager_tx = self.tx.clone();
            tokio::spawn(async move {
                if let Ok(peer) =
                    Peer::connect_from_addr(addr, info_hash, *PEER_ID, peer_manager_tx).await
                    && let Err(peer_error) = peer.run().await
                {
                    println!("Error occurred when connecting to peer: {peer_error}.");
                }
            });
        }
    }

    pub(super) async fn get_tracker_response<'a>(
        &mut self,
        tracker_request: TrackerRequest<'a>,
    ) -> Option<TrackerResponse> {
        for tier in self.announce_list.0.iter_mut() {
            if let Some((url_index_in_tier, tracker_response)) = tracker_request
                .get_first_response_in_list(tier.clone())
                .await
            {
                let url = tier.remove(url_index_in_tier);
                tier.insert(0, url);

                return Some(tracker_response);
            }
        }

        None
    }

    pub(super) fn set_tracker_req_interval(&mut self, timeout: usize) {
        self.tracker_timeout = Duration::from_secs(timeout as u64);
    }
}

impl PeerManager {
    pub(super) async fn req_tracker_add_peers(&mut self, client_options: &ClientOptions) {
        let left_to_download = self.get_bytes_left_to_download();
        let info_hash = self.get_info_hash();

        let tracker_request =
            TrackerRequest::new(&info_hash, PEER_ID, client_options.port, left_to_download);

        if let Some(res) = self
            .peer_fetcher
            .get_tracker_response(tracker_request)
            .await
        {
            self.peer_fetcher
                .add_peers_to_manager(info_hash, res.get_peers())
                .await;
            self.peer_fetcher.set_tracker_req_interval(res.interval);
        } else {
            panic!("Could not get a valid response from the tracker.");
        }
    }

    fn get_bytes_left_to_download(&self) -> u32 {
        let total_pieces = self.piece_selector.get_have().iter().count() as u32;
        let num_pieces_we_have = self
            .piece_selector
            .get_have()
            .iter()
            .fold(0u32, |acc, e| e.then_some(acc + 1).unwrap_or(acc));
        match total_pieces - num_pieces_we_have {
            0 => 999, // the piece_selector will return an empty Vec if we don't know the metainfo yet, so we'll just return anything really
            num_pieces_we_dont_have => num_pieces_we_dont_have * BLOCK_MAX,
        }
    }
}
