use futures_util::FutureExt;
use std::{collections::HashSet, net::SocketAddr, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

use crate::{
    BLOCK_MAX, Peer, PeerManager, TrackerRequest,
    client::{ClientOptions, PEER_ID},
    core::tracker::TrackerResponse,
    peer_manager::ReqMsgFromPeer,
    torrent::{AnnounceList, InfoHash},
};

const DEFAULT_TRACKER_INTERVAL: Duration = Duration::from_secs(120);
/// specifies after how long we should stop the request to the tracker (ideally before tcp connection timeout occurs)
const TRACKER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const NUM_ENOUGH_PEERS_TO_START: usize = 20; // not all of them are reachable

#[derive(Debug)]
pub(super) struct PeerFetcher {
    pub(super) tx: mpsc::Sender<ReqMsgFromPeer>,
    /// something like [ [ tracker1, tracker2 ], [ backup1 ] ]
    pub(super) announce_list: AnnounceList,
    tracker_req_interval: Duration,
}

impl PeerFetcher {
    pub(in crate::peer_manager) fn get_tracker_req_interval(&self) -> Duration {
        self.tracker_req_interval
    }

    pub(super) fn new(tx: mpsc::Sender<ReqMsgFromPeer>, announce_list: AnnounceList) -> Self {
        Self {
            tx,
            announce_list,
            tracker_req_interval: DEFAULT_TRACKER_INTERVAL,
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
                // TODO: error
                match Peer::connect_from_addr(addr, info_hash, *PEER_ID, peer_manager_tx).await {
                    Ok(peer) => {
                        peer.run_gracefully(info_hash).await;
                    }
                    Err(err) => {
                        println!("{err}")
                    }
                }
            });
        }
    }

    pub(super) async fn get_tracker_response<'a>(
        &mut self,
        tracker_request: TrackerRequest<'a>,
    ) -> Option<(TrackerResponse, Vec<SocketAddr>)> {
        // construct the stream which gets the first response of each tier with a timeout
        // essentially mutably returns the peer, the index of the url in the peer and the response
        let tracker_responses = futures_util::stream::FuturesUnordered::from_iter(
            self.announce_list
                .0
                .iter_mut()
                .map(|tier| {
                    tokio::time::timeout(
                        TRACKER_REQUEST_TIMEOUT,
                        tracker_request.get_first_response_in_list(tier.clone()),
                    )
                    .map(move |res| (tier, res))
                })
                .into_iter(),
        );
        tokio::pin!(tracker_responses);

        let mut first_response = None;
        let mut peer_list = HashSet::new();

        while let Some((tier, Ok(Some((url_index_in_tier, tracker_response))))) =
            dbg!(tracker_responses.next().await)
        {
            // extend peer list
            peer_list.extend(tracker_response.get_peers());

            // update tier ranking
            first_response.get_or_insert(tracker_response);
            let url = tier.remove(url_index_in_tier);
            tier.insert(0, url);

            // if we got enough peers, break
            if peer_list.len() >= NUM_ENOUGH_PEERS_TO_START {
                break;
            }
        }

        first_response.map(|res| (res, peer_list.into_iter().collect()))
    }

    pub(super) fn set_tracker_req_interval(&mut self, timeout: usize) {
        self.tracker_req_interval = Duration::from_secs(timeout as u64);
    }
}

impl PeerManager {
    pub(super) async fn req_tracker_add_peers(&mut self, client_options: &ClientOptions) {
        let left_to_download = self.get_bytes_left_to_download();
        let info_hash = self.get_info_hash();

        let tracker_request =
            TrackerRequest::new(&info_hash, PEER_ID, client_options.port, left_to_download);

        if let Some((res, peers)) = self
            .peer_fetcher
            .get_tracker_response(tracker_request)
            .await
        {
            self.peer_fetcher
                .add_peers_to_manager(info_hash, peers)
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
