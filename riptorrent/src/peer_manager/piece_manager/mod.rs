//! The this is where the piece manager lives. He's responsible for writing the block to a file and creating new requests from Pieces.
//! He also handles timeouts
use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

use crate::peer_manager::{
    PieceState, error::PeerManagerError, piece_manager::req_preparer::DownloadQueue,
};
mod file_manager;
pub(super) mod piece_selector;
mod req_preparer;

/// A manager who encapsulates all the stuff relevant for actually downloading pieces and writing them to a file.
/// You can use [`build`] to create a new one.
#[derive(Debug)]
pub(super) struct PieceManager {
    download_queue: DownloadQueue,
    /// the output file
    file: File,
    info_hash_hex: String,
}

impl PieceManager {
    /// we need the info_hash_hex for caching
    pub(super) fn build(
        file_path: PathBuf,
        info_hash_hex: String,
    ) -> Result<Self, PeerManagerError> {
        let file = get_file(file_path)?;
        let download_queue = DownloadQueue::new();

        Ok(Self {
            download_queue,
            file,
            info_hash_hex,
        })
    }
}

fn get_file(path: PathBuf) -> Result<File, PeerManagerError> {
    // TODO: I'm not sure whether thats the best way
    // so if a file doesn't exist _anymore_ then it will just create a new one, although we may want to fail
    let exists = std::fs::exists(&path)?;
    OpenOptions::new()
        .create(!exists)
        .append(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| PeerManagerError::OpenError {
            path,
            error: error.kind(),
        })
}
