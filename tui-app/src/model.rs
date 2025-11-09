use std::{collections::HashMap, path::PathBuf};

use riptorrent::{client::Client, database::FileInfo, events::ApplicationEvent, torrent::InfoHash};
use tui_input::Input;

#[derive(Debug)]
pub struct Model {
    pub client: Client,
    pub torrents: HashMap<InfoHash, TorrentInfo>,
    pub current_page: NavPage,
    pub running: bool,
}

impl Model {
    pub fn new(client: Client, torrents: Vec<FileInfo>) -> Self {
        let torrents = torrents
            .into_iter()
            .map(|torrent| (torrent.info_hash, torrent.into()))
            .collect();

        Self {
            torrents,
            client,
            running: true,
            current_page: Default::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct TorrentInfo {
    /// size of the file(s) in bytes
    pub size: usize,
    pub number_pieces: usize,
    pub file_path: PathBuf,
    pub bitfield: Vec<bool>,
    pub peer_connections: PeerConnections,
}

#[derive(Debug)]
pub struct PeerConnections {
    inbound: u8,
    outbound: u8,
}

impl From<FileInfo> for TorrentInfo {
    fn from(value: FileInfo) -> Self {
        TorrentInfo {
            size: value.size,
            number_pieces: value.number_pieces,
            file_path: value.file_path,
            bitfield: value.bitfield,
            ..Default::default()
        }
    }
}

impl Default for PeerConnections {
    fn default() -> Self {
        Self {
            inbound: Default::default(),
            outbound: Default::default(),
        }
    }
}

impl PeerConnections {
    pub fn increase_inbound(&mut self, i: i8) {
        self.inbound = self.inbound.saturating_add_signed(i)
    }

    pub fn increase_outbound(&mut self, i: i8) {
        self.outbound = self.outbound.saturating_add_signed(i)
    }
}

#[derive(Debug, Default)]
pub enum NavPage {
    #[default]
    TorrentList,
    AddingTorrent(Input),
}

pub enum Message {
    ApplicationEvent(ApplicationEvent),
    GoToMainPage,

    /// if we press something to get to the popup
    InitAddTorrent,
    /// after we entered something
    AddTorrent(TorrentType),

    Quit,
}

#[derive(Debug)]
pub enum TorrentType {
    TorrentPath(PathBuf),
    MagnetLink(url::Url),
}

impl TryFrom<&str> for TorrentType {
    type Error = TorrentParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Ok(url) = url::Url::parse(value) {
            Ok(Self::MagnetLink(url))
        } else if let Ok(path) = PathBuf::try_from(value)
            && std::fs::exists(&path).is_ok_and(|exists| exists)
        {
            Ok(Self::TorrentPath(path))
        } else {
            Err(TorrentParseError())
        }
    }
}

pub struct TorrentParseError();
