use tui_input::Input;

use crate::{
    model::{Message, Model, TorrentType},
    update::torrent_event::update_from_application_event,
};

pub(super) mod torrent_event;
pub(super) mod user_input;

pub(crate) async fn update(model: &mut Model, msg: Message) {
    match msg {
        Message::GoToMainPage => model.go_to_main_page(),
        Message::Quit => model.running = false,
        Message::InitAddTorrent => {
            model.current_page = crate::model::NavPage::AddingTorrent(Input::new("".to_string()))
        }
        Message::AddTorrent(torrent_type) => {
            match torrent_type {
                TorrentType::TorrentPath(path_buf) => model
                    .client
                    .add_torrent(&path_buf, None)
                    .await
                    .expect("failed to add torrent file"),
                TorrentType::MagnetLink(url) => model
                    .client
                    .add_magnet(url.as_str(), None)
                    .await
                    .expect("failed to add magnet link"),
            };
            model.go_to_main_page();
            // model.running = false;
        }
        Message::ApplicationEvent(application_event) => {
            update_from_application_event(model, application_event)
        }
        Message::PauseResumeTorrent => {
            let Some(torrent_info) = model
                .get_torrents()
                .get(model.list_state.absolute_selection_index())
            else {
                return;
            };

            match torrent_info.is_paused {
                true => model.client.resume_download(&torrent_info.info_hash).await,
                false => model.client.pause_download(&torrent_info.info_hash).await,
            };
        }
    }
}

impl Model {
    fn go_to_main_page(&mut self) {
        self.current_page = crate::model::NavPage::TorrentList
    }
}
