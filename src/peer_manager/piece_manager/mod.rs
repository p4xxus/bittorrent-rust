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

#[derive(Debug)]
pub(super) struct PieceManager {
    /// if it's None, we are finished
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
        exists: bool,
    ) -> Result<Self, PeerManagerError> {
        let file = get_file(file_path, exists)?;
        let download_queue = DownloadQueue::new();

        Ok(Self {
            download_queue,
            file,
            info_hash_hex,
        })
    }
}
fn get_file(path: PathBuf, exists: bool) -> Result<File, PeerManagerError> {
    OpenOptions::new()
        .create(exists)
        .append(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| PeerManagerError::OpenError { path, error })
}
