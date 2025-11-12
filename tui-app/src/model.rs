use std::path::PathBuf;

use log::warn;
use riptorrent::{client::Client, database::FileInfo, events::ApplicationEvent, torrent::InfoHash};
use tui_input::Input;

#[derive(Debug)]
pub struct Model {
    pub client: Client,
    torrents: Vec<TorrentInfo>,

    // states
    pub current_page: NavPage,
    pub running: bool,
    pub list_state: TorrentListState,
}

impl Model {
    pub fn new(client: Client, torrents: Vec<FileInfo>) -> Self {
        let torrents: Vec<TorrentInfo> = torrents.into_iter().map(Into::into).collect();
        let num_torrents = torrents.len();

        Self {
            torrents,
            client,
            running: true,
            current_page: Default::default(),
            list_state: TorrentListState::new(num_torrents),
        }
    }

    pub fn get_torrents(&self) -> &Vec<TorrentInfo> {
        &self.torrents
    }

    pub fn push_torrent_info(&mut self, torrent_info: TorrentInfo) {
        self.torrents.push(torrent_info);
        self.list_state.total_items += 1;
    }

    pub fn get_torrent_from_info_hash_mut(
        &mut self,
        info_hash: &InfoHash,
    ) -> Option<&mut TorrentInfo> {
        self.torrents
            .iter_mut()
            .find(|info| &info.info_hash == info_hash)
    }
}

#[derive(Debug)]
pub struct TorrentInfo {
    /// size of the file(s) in bytes
    pub info_hash: InfoHash,
    pub size: u64,
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
            info_hash: value.info_hash,
            size: value.size as u64,
            file_path: value.file_path,
            bitfield: value.bitfield,
            peer_connections: PeerConnections::default(),
        }
    }
}

impl TorrentInfo {
    pub fn num_pieces(&self) -> usize {
        self.bitfield.len()
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
    pub fn inbound(&self) -> u8 {
        self.inbound
    }

    pub fn outbound(&self) -> u8 {
        self.outbound
    }

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
            Err(TorrentParseError)
        }
    }
}

pub struct TorrentParseError;

#[derive(Debug)]
pub struct TorrentListState {
    /// Index of the selected item
    selected: Option<usize>,
    /// Scroll offset (top visible item index)
    pub offset: usize,
    /// Total number of items in the list
    pub total_items: usize,
}

impl TorrentListState {
    pub fn new(total_items: usize) -> Self {
        Self {
            selected: (total_items > 0).then_some(0),
            offset: 0,
            total_items,
        }
    }

    pub fn next(&mut self) {
        if let Some(i) = self.selected {
            self.selected = Some((i + 1) % self.total_items);
        }
    }

    pub fn previous(&mut self) {
        if let Some(i) = self.selected {
            self.selected = Some((i + self.total_items - 1) % self.total_items);
        }
    }

    pub(crate) fn get_visible_item_range(
        &mut self,
        max_visible_items: usize,
    ) -> std::ops::Range<usize> {
        self.scroll_to_selected(max_visible_items);

        // Determine the slice of data to render
        let start_index = self.offset;
        let end_index = (self.offset + max_visible_items).min(self.total_items);
        start_index..end_index
    }

    /// Returns the relative selected index of this [`TorrentListState`].
    pub(crate) fn relative_selected_index(&self) -> usize {
        self.selected.unwrap_or_default() - self.offset
    }

    // This is the core logic: adjust offset to ensure selected item is visible
    fn scroll_to_selected(&mut self, max_visible_items: usize) {
        if let Some(selected_index) = self.selected {
            // Adjust offset to scroll down if selected item is off-screen
            if selected_index >= self.offset + max_visible_items
                && selected_index != self.total_items
            {
                self.offset = selected_index - max_visible_items + 1;
            // If selected item is above the current view, adjust offset
            } else if selected_index < self.offset {
                self.offset = selected_index;
            }
            // if i < self.offset {
            //     self.offset = i;
            // }
            // The max_visible_lines logic needs to be calculated dynamically in your rendering function
            // This is just a placeholder for the concept:
            // if i >= self.offset + max_visible_lines {
            //     self.offset = i - max_visible_lines + 1;
            // }
        }
    }
}
