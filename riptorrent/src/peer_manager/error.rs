use std::{error::Error, io, path::PathBuf};

use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

use crate::{
    database::DBError, extensions::magnet_links::MagnetLinkError, peer_manager::ResMessage,
    torrent::TorrentError,
};
#[derive(Debug, Error)]
pub enum PeerManagerError {
    #[error("The request-manager failed with the following DB error: {0}")]
    DB(#[from] DBError),
    #[error("The request-manager failed with the following Torrent error: {0}")]
    Torrent(#[from] TorrentError),
    #[error("The request-manager failed with the following magnet-link error: {0}")]
    MagnetLink(#[from] MagnetLinkError),
    #[error("Failed to open the file at the path `{path}` with the error: `{error}`")]
    OpenError { path: PathBuf, error: io::ErrorKind },
    #[error(
        "Failed to send a message: `{msg}` to peer with ID {peer_id:?} with the error: `{error}`"
    )]
    SendError {
        peer_id: [u8; 20],
        error: SendError<ResMessage>,
        msg: String,
    },
    #[error("An error occured when writing to the file: `{0}`")]
    WritingToFile(#[from] io::Error),
    #[error("No file name provided")]
    NoFileName,
    #[error("Some other error occured: `{0}`")]
    Other(Box<dyn Error + Send + Sync>),
}
