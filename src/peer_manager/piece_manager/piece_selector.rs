//! This whole thing is merely existent for selecting the next pieces we might send to peers.
//! The actual job of creating requests, handling timeouts is done in the `req_preparer.rs`

use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    vec,
};

use crate::peer_manager::PeerId;

/// Responsible for selecting the next piece:
/// - implements the rarest-first strategy
/// - if same rarity, goes by index
///
/// 1.  We **keep** the `piece_rarity: Vec<u32>` as the single source of truth for piece rarity.
/// 2.  When a piece's rarity is updated (e.g., a peer connects and has the piece), we simply **push a new entry** `(new_rarity, piece_index)` into the `BinaryHeap`. We do **not** remove the old entry.
/// 3.  When we call `select_pieces_for_peer`, we pop an item (rarity_in_heap, piece_index)` from the heap.
/// 4.  **Crucially, we then check if `rarity_in_heap` matches the current, true rarity in `self.piece_rarity[piece_index]`.**
///     *   If they don't match, it means this heap entry is **stale**. We discard it and pop the next one.
///     *   If they match, this is a valid, up-to-date entry. We process it.

// This "lazy" approach has some great benefits:
// *   **Correctness:** We only ever act on the most up-to-date rarity information.
// *   **Simplicity:** We only use the standard library's `BinaryHeap` and a `Vec`. No complex data structures needed.
// *   **Good Performance:** The heap might grow larger with stale entries, but heap operations are logarithmic (`log N`). In practice, the performance is excellent, and the stale entries are cleaned out as they are encountered. The cost of pushing a new entry is small.
#[derive(Debug)]
pub(in crate::peer_manager) struct PieceSelector {
    /// A vector where the index is the piece index and the value
    /// is the number of peers that have this piece.
    piece_rarity: Vec<u32>,

    /// A priority queue of pieces to download.
    /// The tuple is (rarity, piece_index).
    // (a,b)>(c,d) if a>c OR (a=c AND b>d) which makes this work as it should
    priority_queue: BinaryHeap<Reverse<(u32, u32)>>,

    /// Our own bitfield, to know which pieces we already have.
    pub(super) have: Vec<bool>,

    /// A map to store the bitfield for each peer.
    /// The key is the PeerId.
    peer_bitfields: HashMap<PeerId, Vec<bool>>,

    /// Tracks pieces that are currently in the DownloadQueue.
    /// This prevents us from selecting the same piece multiple times.
    /// A Vec<bool> is likely more efficient than a HashSet<u32> if piece indices are contiguous.
    pieces_in_flight: Vec<bool>,
}

impl PieceSelector {
    pub(in crate::peer_manager) fn new(have: Vec<bool>) -> Self {
        let n_pieces = have.len();
        Self {
            piece_rarity: vec![0; n_pieces],
            priority_queue: BinaryHeap::with_capacity(n_pieces),
            have,
            peer_bitfields: HashMap::new(),
            pieces_in_flight: vec![false; n_pieces],
        }
    }

    /// since we might not possess the correct bitfield of ourselves, we need to initialize this first with an empty length
    /// and then we can initialize it for real.
    ///
    /// The `bitfield` must be the correct length
    pub(in crate::peer_manager) fn update_from_self_have(&mut self, bitfield: Vec<bool>) {
        let n_pieces = bitfield.len();

        // set our own have bitfield
        self.have = bitfield;

        // update peer bitfields to be correct len
        for peer_bitfields in self.peer_bitfields.values_mut() {
            peer_bitfields.resize(n_pieces, false);
        }

        // recalculate piece_rarity
        self.piece_rarity =
            self.peer_bitfields
                .values()
                .fold(vec![0u32; n_pieces], |acc, bitfield| {
                    acc.iter()
                        .zip(bitfield)
                        .map(|(count, b)| match *b {
                            true => count + 1,
                            false => *count,
                        })
                        .collect()
                });

        // update priority queue
        for (id, bitfield) in self.peer_bitfields.clone().into_iter() {
            self.add_peer_bitfield(&id, bitfield);
        }

        // allocate space for the pieces in flight
        self.pieces_in_flight.resize(n_pieces, false);
    }

    /// use this function to add a new peer with an empty bitfield to this selector
    pub(in crate::peer_manager) fn add_peer(&mut self, id: PeerId) {
        let n_pieces = (!self.have.is_empty())
            .then_some(self.have.len())
            .or_else(|| {
                self.peer_bitfields
                    .values()
                    .next()
                    .map(|bitfield| bitfield.len())
            })
            .unwrap_or(0);
        if let Some(old) = self.peer_bitfields.insert(id, vec![false; n_pieces]) {
            // this shouldn't happen, but if it does we're safe
            self.peer_bitfields.insert(id, old);
        };
    }

    /// removes the peer from the list and the priority queue
    pub(in crate::peer_manager) fn remove_peer(&mut self, id: &PeerId) {
        if let Some(bitfield) = self.peer_bitfields.remove(id) {
            self.update_prio_queue(bitfield, false);
        }
    }

    /// updates the priority queue if the peer sent us his bitfield
    /// if the piece is not added via `add_peer` yet, it will do nothing
    pub(in crate::peer_manager) fn add_peer_bitfield(&mut self, id: &PeerId, bitfield: Vec<bool>) {
        if bitfield.is_empty() {
            return;
        }
        // this might be the first bitfield we get and so we must update the lengths of the other bitfields
        self.peer_bitfields
            .values_mut()
            .filter(|bitfield| bitfield.is_empty())
            .for_each(|other_bitfield| other_bitfield.resize(bitfield.len(), false));

        if let Some(peer_bitfield) = self.peer_bitfields.get_mut(id) {
            *peer_bitfield = bitfield.clone();
            self.update_prio_queue(bitfield, true);
        }
    }

    /// updates the priority queue if the peer sent us a have message
    pub(in crate::peer_manager) fn update_from_peer_have(&mut self, id: &PeerId, have_index: u32) {
        // for easier indexing
        let i = have_index as usize;
        if let Some(peer_bitfield) = self.peer_bitfields.get_mut(id)
            && let Some(b_peer) = peer_bitfield.get_mut(i)
            && let Some(rarity) = self.piece_rarity.get_mut(i)
        {
            *b_peer = true;
            *rarity += 1;
            if self.have.get(i).is_some_and(|b| !b) {
                self.priority_queue.push(Reverse((*rarity, have_index)));
            }
        }
    }

    // checks in order:
    // - (does bitfield for peer exist)
    // - is there a next piece from the priority queue
    // - (if yes) is the piece even up-to-date
    // - (if yes) does the peer even have that piece
    // - (if yes) is the piece already requested by any peer
    /// selects `count` amount of pieces from a peer so we can request them
    pub(super) fn select_pieces_for_peer(&mut self, id: &PeerId, count: usize) -> Option<Vec<u32>> {
        let bitfield = self.peer_bitfields.get(id)?;
        let mut queue = Vec::with_capacity(count);
        while queue.len() <= count
            && let Some(Reverse((count, piece_i))) = self.priority_queue.pop()
        {
            let i = piece_i as usize;
            // the priority queue doesn't have to be up to date since we don't remove values if we receive e.g. a have message
            if self.piece_rarity[i] == count
                && let Some(b_p) = bitfield.get(i)
                && *b_p
                && !self.pieces_in_flight[i]
            {
                queue.push(piece_i);
                self.pieces_in_flight[i] = true;
            }
        }
        if queue.is_empty() { None } else { Some(queue) }
    }

    /// updates both priority queues with a bitfield and whether it should increase or decrease the count
    fn update_prio_queue(&mut self, bitfield: impl IntoIterator<Item = bool>, increase: bool) {
        if self.have.is_empty() {
            return;
        }
        self.piece_rarity
            .iter_mut()
            .enumerate()
            .zip(bitfield.into_iter().zip(&self.have))
            .for_each(|((i, count), (b_p, b_i))| {
                if b_p && !b_i {
                    match increase {
                        true => *count += 1,
                        false => *count = count.saturating_sub(1),
                    }
                    self.priority_queue.push(Reverse((*count, i as u32)));
                }
            });
    }

    pub(in crate::peer_manager) fn get_peer_has(&self, id: &PeerId) -> Option<&Vec<bool>> {
        self.peer_bitfields.get(id)
    }

    pub(in crate::peer_manager) fn get_have(&self) -> &Vec<bool> {
        &self.have
    }
}
