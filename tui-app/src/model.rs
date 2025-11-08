use std::path::PathBuf;

use riptorrent::{client::Client, database::FileInfo, events::ApplicationEvent};
use tui_input::Input;

#[derive(Debug)]
pub struct Model {
    pub client: Client,
    pub torrents: Vec<FileInfo>,
    pub page: NavPage,
    pub running: bool,
}

impl Model {
    pub fn new(client: Client, torrents: Vec<FileInfo>) -> Self {
        Self {
            torrents,
            client,
            running: true,
            page: Default::default(),
        }
    }

    pub fn go_to_main_page(&mut self) {
        self.page = crate::model::NavPage::TorrentList
    }
}

#[derive(Debug, Default)]
pub enum NavPage {
    #[default]
    TorrentList,
    AddingTorrent(Input),
}

pub enum Message {
    GoToMainPage,
    ApplicationEvent(ApplicationEvent),

    InitAddTorrent,
    AddTorrent(TorrentType),

    Quit,
}

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
