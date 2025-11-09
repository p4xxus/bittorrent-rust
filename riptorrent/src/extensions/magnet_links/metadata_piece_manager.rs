//! This is all for the PeerManager
//! It contains the types for requesting the metainfo.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use sha1::{Digest, Sha1};

use crate::{
    magnet_links::metadata_msg::{MetadataMsg, MetadataMsgType},
    peer_manager::BlockState,
    torrent::{InfoHash, Metainfo},
};

/// The metadata is handled in blocks of 16KiB (16384 Bytes).
const METADATA_BLOCK_SIZE: usize = 1 << 14;
const TIMEOUT_METADATA_REQ: Duration = Duration::from_secs(5);

/// A struct similar to the normal PieceManager that's running before the actual PieceManager.
/// It's responsible for requesting blocks of metadata and constructing the metainfo itself.
#[derive(Debug)]
pub(crate) struct MetadataPieceManager {
    queue: Vec<BlockState>,
    bytes: BytesMut,
    info_hash: InfoHash,
}

impl MetadataPieceManager {
    pub(crate) fn new(info_hash: InfoHash) -> Self {
        Self {
            queue: Vec::new(),
            bytes: BytesMut::new(),
            info_hash,
        }
    }

    pub(crate) fn add_block(&mut self, index: u32, data: Bytes) {
        let len = data.len();
        let begin = index as usize * METADATA_BLOCK_SIZE;
        self.bytes[begin..begin + len].copy_from_slice(&data);
        self.queue[index as usize] = BlockState::Finished;
    }

    /// returns a Vec of serialized metadata request messages
    pub(crate) fn get_block_req_data(&mut self) -> Vec<Bytes> {
        self
            .queue
            .iter_mut()
            .enumerate()
            .filter_map(|(piece_i, i_have)| {
                if *i_have == BlockState::None
                    || matches!(i_have, BlockState::InProcess(instant_requested) if instant_requested.elapsed() >= TIMEOUT_METADATA_REQ)
                {
                    *i_have = BlockState::InProcess(std::time::Instant::now());

                    Some(Bytes::from_owner(serde_bencode::to_bytes(&MetadataMsg {
                        msg_type: MetadataMsgType::Request,
                        piece_index: piece_i as u32,
                        total_size: None,
                    }).expect("serializing this should always work")))
                } else {
                    None
                }
            }).collect()
        // Note that one peer will not get the same request twice since it won't ask for another if it's waiting for the response of one.
        // If it got the response, it won't get the piece again anyways.
    }

    /// initializes the fields of the MetadataPieceManager (like which blocks are finished)
    /// with the given length
    pub(crate) fn set_len(&mut self, length: usize) {
        if !self.queue.is_empty() {
            // it's already initialized
            return;
        }
        let n_blocks = length.div_ceil(METADATA_BLOCK_SIZE);
        self.queue = vec![BlockState::None; n_blocks];
        self.bytes = BytesMut::zeroed(length);
    }

    pub(crate) fn check_finished(&mut self) -> bool {
        if self.queue.iter().all(|i| *i == BlockState::Finished) {
            let mut hasher = Sha1::new();
            hasher.update(&self.bytes);
            let sha1: [u8; 20] = hasher.finalize().into();

            if sha1 == *self.info_hash {
                return true;
            } else {
                self.queue = vec![BlockState::None; self.queue.len()];
            }
        }
        false
    }

    pub(crate) fn get_metadata(&self) -> Result<Metainfo, serde_bencode::Error> {
        serde_bencode::from_bytes(&self.bytes)
    }

    pub(crate) fn get_info_hash(&self) -> InfoHash {
        self.info_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::InfoHash;

    #[test]
    fn test_new_metadata_piece_manager() {
        let info_hash = InfoHash([0x00; 20]);
        let manager = MetadataPieceManager::new(info_hash);
        assert!(manager.queue.is_empty());
        assert_eq!(manager.bytes.len(), 0);
        assert_eq!(manager.info_hash, info_hash);
    }

    #[test]
    fn test_set_len() {
        let info_hash = InfoHash([0x00; 20]);
        let mut manager = MetadataPieceManager::new(info_hash);

        // Test initialization
        manager.set_len(METADATA_BLOCK_SIZE * 3 + 100); // 4 blocks
        assert_eq!(manager.queue.len(), 4);
        assert!(manager.queue.iter().all(|&b| b == BlockState::None));
        assert_eq!(manager.bytes.len(), METADATA_BLOCK_SIZE * 3 + 100);
        assert!(manager.bytes.iter().all(|b| *b == 0));

        // Test that calling set_len again does nothing if already initialized
        manager.queue[0] = BlockState::Finished; // Mark one piece as received
        manager.set_len(100);
        assert_eq!(manager.queue.len(), 4); // Should still be 4, not 1
        assert_eq!(manager.queue[0], BlockState::Finished); // Should still be true
    }

    #[test]
    fn test_add_block() {
        let info_hash = InfoHash([0x00; 20]);
        let mut manager = MetadataPieceManager::new(info_hash);
        manager.set_len(METADATA_BLOCK_SIZE * 2 + 3); // 3 blocks

        let block_data_0 = Bytes::from_owner([0x01; METADATA_BLOCK_SIZE]);
        let block_data_1 = Bytes::from_owner([0x02; METADATA_BLOCK_SIZE]);

        manager.add_block(0, block_data_0.clone());
        assert_eq!(manager.queue[0], BlockState::Finished);
        assert_eq!(&manager.bytes[0..METADATA_BLOCK_SIZE], block_data_0);

        manager.add_block(1, block_data_1.clone());
        assert_eq!(manager.queue[1], BlockState::Finished);
        assert_eq!(
            &manager.bytes[METADATA_BLOCK_SIZE..METADATA_BLOCK_SIZE * 2],
            block_data_1
        );

        manager.add_block(2, Bytes::from_owner([1, 2, 3]));
        assert_eq!(manager.queue[2], BlockState::Finished);
        assert_eq!(
            &manager.bytes[METADATA_BLOCK_SIZE * 2..METADATA_BLOCK_SIZE * 2 + 3],
            &[1, 2, 3]
        );
    }

    #[test]
    fn test_get_block_req_data() {
        let info_hash = InfoHash([0x00; 20]);
        let mut manager = MetadataPieceManager::new(info_hash);
        manager.set_len(METADATA_BLOCK_SIZE * 3); // 3 blocks

        // Request first block
        let req_data_0 = manager.get_block_req_data();
        assert_eq!(
            req_data_0,
            vec![
                Bytes::from_owner(
                    serde_bencode::to_bytes(&MetadataMsg {
                        msg_type: MetadataMsgType::Request,
                        piece_index: 0,
                        total_size: None
                    })
                    .unwrap()
                ),
                serde_bencode::to_bytes(&MetadataMsg {
                    msg_type: MetadataMsgType::Request,
                    piece_index: 1,
                    total_size: None
                })
                .unwrap()
                .into(),
                serde_bencode::to_bytes(&MetadataMsg {
                    msg_type: MetadataMsgType::Request,
                    piece_index: 2,
                    total_size: None
                })
                .unwrap()
                .into()
            ]
        );
        assert!(matches!(manager.queue[0], BlockState::InProcess(_)));
        assert!(matches!(manager.queue[1], BlockState::InProcess(_)));
        assert!(matches!(manager.queue[2], BlockState::InProcess(_)));

        manager.queue[0] = BlockState::Finished;
        manager.queue[1] = BlockState::Finished;
        manager.queue[2] = BlockState::Finished;

        // Should be empty when all blocks are received
        assert!(manager.get_block_req_data().is_empty());
    }

    #[test]
    fn test_check_finished() {
        let metadata_bytes = BytesMut::from(&[0x01; METADATA_BLOCK_SIZE][..]);
        let mut hasher = Sha1::new();
        hasher.update(&metadata_bytes);
        let info_hash = InfoHash(hasher.finalize().into());
        let mut manager = MetadataPieceManager::new(info_hash);
        manager.set_len(METADATA_BLOCK_SIZE);

        manager.bytes = metadata_bytes;
        manager.queue = vec![BlockState::Finished; 1];

        assert!(manager.check_finished());

        // simulate not matching metainfo
        manager.bytes = BytesMut::from(&[0x00; METADATA_BLOCK_SIZE][..]);
        assert!(!manager.check_finished());
        // Queue should be reset
        assert!(manager.queue.iter().all(|&b| b == BlockState::None));
    }

    #[test]
    fn test_check_finished_not_all_pieces() {
        let info_hash = InfoHash([0x00; 20]);
        let mut manager = MetadataPieceManager::new(info_hash);
        manager.set_len(METADATA_BLOCK_SIZE * 2);

        let bytes = vec![0x00; METADATA_BLOCK_SIZE];
        manager.bytes = BytesMut::from(&[0x00; METADATA_BLOCK_SIZE][..]);
        manager.queue[0] = BlockState::Finished; // Only one piece received
        assert!(!manager.check_finished());
        // queue should still be the same
        assert_eq!(*manager.bytes, bytes);
    }

    #[test]
    fn test_get_metadata() {
        // let info_hash = InfoHash([0x00; 20]);
        // let mut manager = MetadataPieceManager::new(info_hash);
        // manager.set_len(10);

        // let bencoded_metainfo = b"4:infod4:name12:a test file12:piece lengthi32768e6e";
        // manager.bytes = Cursor::new(bencoded_metainfo.to_vec());

        // let metainfo = manager.get_metadata().unwrap();
        // assert_eq!(metainfo.name, "a test file");
        // assert_eq!(metainfo.piece_length, 32768);
    }
}
