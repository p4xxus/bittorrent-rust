use std::borrow::Cow;
use std::path::Path;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use surrealdb::RecordId;
use surrealdb::Surreal;
use surrealdb::engine::local::Db;
use surrealdb::opt::PatchOp;

// For a RocksDB file
use surrealdb::engine::local::RocksDb;
use thiserror::Error;

use crate::torrent::AnnounceList;
use crate::torrent::InfoHash;
use crate::torrent::Metainfo;

/// the actual data stored in the DB
/// torrent path is also the key
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct DBEntry {
    pub(crate) bitfield: Cow<'static, [bool]>,
    pub(crate) file: Cow<'static, Path>,
    pub(crate) torrent_info: Metainfo,
    pub(crate) announce_list: AnnounceList,
}

/// The naming might be confusing (why not TorrentInfo) but
/// this represents the public struct to use when we want to have access to things like the length of the torrent
/// in the end the public view onto the DBEntry
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub info_hash: InfoHash,
    /// size of the file(s) in bytes
    pub size: usize,
    /// number of bytes in each piece
    pub piece_size: u32,
    pub file_path: PathBuf,
    pub bitfield: Vec<bool>,
}

impl From<DBEntry> for FileInfo {
    fn from(value: DBEntry) -> Self {
        let size = value.torrent_info.get_length() as usize;
        let piece_size = value.torrent_info.piece_length;

        FileInfo {
            info_hash: value.torrent_info.info_hash(),
            size,
            piece_size,
            file_path: value.file.to_path_buf(),
            bitfield: value.bitfield.to_vec(),
        }
    }
}

impl DBEntry {
    fn from_new_file(file_path: PathBuf, info: Metainfo, announce_list: AnnounceList) -> Self {
        let n_pieces = info.pieces.0.len();
        Self {
            bitfield: (vec![false; n_pieces]).into(),
            file: file_path.into(),
            torrent_info: info,
            announce_list,
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.bitfield.iter().all(|b| *b)
    }
}

#[derive(Debug, Deserialize)]
struct Record {
    #[allow(dead_code)]
    id: RecordId,
}
#[derive(Debug, Clone)]
pub(crate) struct SurrealDbConn {
    pub(crate) db: Surreal<Db>,
}

impl SurrealDbConn {
    pub(crate) async fn new(db_name: &str) -> Result<SurrealDbConn, DBError> {
        let db = Surreal::new::<RocksDb>(db_name).await?;
        db.use_ns("files_ns").use_db("files_db").await?;
        Ok(Self { db })
    }

    pub(crate) async fn get_entry(&self, info_hash_hex: &str) -> Result<Option<DBEntry>, DBError> {
        let entry = self.db.select(("files", info_hash_hex)).await?;
        Ok(entry)
    }

    pub(crate) async fn get_all(&self) -> Vec<DBEntry> {
        self.db.select("files").await.unwrap_or_default()
    }

    pub(crate) async fn set_entry(
        &self,
        file_path: PathBuf,
        info: Metainfo,
        announce_list: AnnounceList,
    ) -> Result<DBEntry, DBError> {
        let info_hash_hex = hex::encode(info.info_hash().0);
        let file = DBEntry::from_new_file(file_path, info, announce_list);
        let entry = self
            .db
            .create::<Option<DBEntry>>(("files", &info_hash_hex))
            .content(file)
            .await?
            .expect("I'm really curious what the error is here.");
        Ok(entry)
    }

    pub(super) async fn update_bitfields(
        &self,
        info_hash_hex: &str,
        new_bitfield: Vec<bool>,
    ) -> Result<(), DBError> {
        let updated: Option<DBEntry> = self
            .db
            .update(("files", info_hash_hex))
            .patch(PatchOp::replace("/bitfield", new_bitfield))
            .await?;

        assert!(
            updated.is_some(),
            "The record for the torrent was already created if wasn't there."
        );

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum DBError {
    #[error("Got error from the local DB: `{0}`")]
    DBError(Box<surrealdb::Error>),
}

impl From<surrealdb::Error> for DBError {
    fn from(value: surrealdb::Error) -> Self {
        Self::DBError(Box::new(value))
    }
}
