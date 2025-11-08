use bytes::BytesMut;

use crate::{
    BLOCK_MAX,
    messages::payloads::RequestPiecePayload,
    peer_manager::{
        BlockState, MAX_PIECES_IN_PARALLEL, PeerId, PieceManager, PieceSelector, PieceState,
    },
    torrent::Metainfo,
};

impl PieceManager {
    /// Returns a list of `n` blocks that we want to request for a given peer by asking the piece selector.
    pub(in crate::peer_manager) fn prepare_next_blocks(
        &mut self,
        piece_selector: &mut PieceSelector,
        n: usize,
        peer_id: &PeerId,
        metainfo: &Metainfo,
    ) -> Option<Vec<RequestPiecePayload>> {
        let mut requests = Vec::with_capacity(n);
        while requests.len() < n
            && let Some(peer_has) = piece_selector.get_peer_has(peer_id)
        {
            let Some(piece) = self.download_queue.get_piece_for_peer(peer_has) else {
                if let Some(queue) =
                    piece_selector.select_pieces_for_peer(peer_id, MAX_PIECES_IN_PARALLEL)
                {
                    self.download_queue.add_pieces_to_queue(queue, metainfo);
                    continue;
                } else {
                    break;
                }
            };

            let num_blocks_for_piece = n - requests.len();
            requests.append(&mut piece.prepare_requests_for_piece(num_blocks_for_piece));
        }
        if requests.is_empty() {
            None
        } else {
            Some(requests)
        }
    }
}

/// represents a queue of current pieces we download
#[derive(Debug)]
pub(super) struct DownloadQueue(pub(super) Vec<PieceState>);

impl DownloadQueue {
    pub(super) fn new() -> Self {
        Self(Vec::with_capacity(MAX_PIECES_IN_PARALLEL))
    }

    /// looks into the queue if there's a piece that the peer has and returns it (if it found one)
    fn get_piece_for_peer(&mut self, peer_has: &[bool]) -> Option<&mut PieceState> {
        // 1. Try if we have something in the download queue
        let piece_i = self.0.iter().position(|state| {
            let Some(peer_has_it) = peer_has.get(state.piece_i as usize) else {
                // this happens if a peer has not yet sent his bitfield which makes it empty
                return false;
            };
            let blocks_we_need = state.blocks.iter().filter(|b| b.is_not_requested());
            *peer_has_it && blocks_we_need.count() >= 1
        })?;
        // TODO: if we didn't find the piece here, we'll run the piece_selector and call this function again
        // however it has to recompute the pieces so we might want to find a better solution here

        // the download_queue will have a last piece, because it may have been added by self.add_piece_to_queue. If it hasn't, we have returned.
        Some(self.0.get_mut(piece_i).expect("we checked that before"))
    }

    /// Really just adds the pieces given into the queue.
    /// It also needs the metainfo to calculate how many bytes the piece is.
    pub(in crate::peer_manager) fn add_pieces_to_queue(
        &mut self,
        pieces: Vec<u32>,
        metainfo: &Metainfo,
    ) {
        for piece_i in pieces.into_iter() {
            let piece_state = PieceState::new(metainfo, piece_i);
            self.0.push(piece_state);
        }
    }
}

impl PieceState {
    /// calculates n_blocks and piece_size and creates a new PieceState
    pub(in crate::peer_manager) fn new(torrent_info: &Metainfo, piece_i: u32) -> Self {
        let piece_size = get_piece_size(torrent_info, piece_i);
        let n_blocks = piece_size.div_ceil(BLOCK_MAX);

        PieceState {
            blocks: vec![BlockState::None; n_blocks as usize],
            piece_i,
            buf: BytesMut::zeroed(piece_size as usize),
        }
    }

    /// Constructs a list of request payloads (block-wise) from a whole piece.
    /// Filters the one already requested out
    fn prepare_requests_for_piece(&mut self, n: usize) -> Vec<RequestPiecePayload> {
        let n_blocks = self.blocks.capacity() as u32;
        let piece_size = self.buf.capacity() as u32;

        self.blocks
            .iter_mut()
            .enumerate()
            .filter(|(_, b)| b.is_not_requested())
            .take(n)
            .map(|(block_i, block)| {
                let block_i = block_i as u32;
                let index = self.piece_i;
                let begin = block_i * BLOCK_MAX;
                let length = get_block_len(n_blocks, piece_size, block_i);

                *block = BlockState::InProcess(std::time::Instant::now());
                RequestPiecePayload::new(index, begin, length)
            })
            .collect()
    }
}

fn get_piece_size(torrent_info: &Metainfo, piece_i: u32) -> u32 {
    let length = torrent_info.get_length();
    let piece_length = torrent_info.piece_length;
    if piece_i == torrent_info.pieces.0.len() as u32 - 1 && length % piece_length != 0 {
        length % piece_length
    } else {
        piece_length
    }
}

fn get_block_len(n_blocks: u32, piece_size: u32, block_i: u32) -> u32 {
    if block_i == n_blocks - 1 && piece_size % BLOCK_MAX != 0 {
        piece_size % BLOCK_MAX
    } else {
        BLOCK_MAX
    }
}
