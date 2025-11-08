use std::time::Duration;

use ratatui::crossterm::event::{self, Event};
use riptorrent::events::ApplicationEvent;
use tokio::sync::broadcast;
use tui_input::{Input, StateChanged, backend::crossterm::EventHandler};

use crate::model::{Message, Model, TorrentType};

pub fn handle_client_event(
    _: &Model,
    rx: &mut broadcast::Receiver<ApplicationEvent>,
) -> Option<Message> {
    if let Ok(event) = rx.try_recv() {
        Some(Message::ApplicationEvent(event))
    } else {
        None
    }
}

/// I need mutable access to the model to update the input field
pub fn handle_user_input(model: &mut Model) -> color_eyre::Result<Option<Message>> {
    if event::poll(Duration::from_millis(250))? {
        let event = event::read()?;
        match &mut model.page {
            crate::model::NavPage::TorrentList => {
                if let Event::Key(key) = event
                    && key.kind == event::KeyEventKind::Press
                {
                    return Ok(handle_key(key));
                }
            }
            crate::model::NavPage::AddingTorrent(input) => {
                if input.handle_event(&event).is_none()
                    && let Event::Key(key) = event
                {
                    match key.code {
                        event::KeyCode::Enter => match TorrentType::try_from(input.value()) {
                            Ok(torrent_type) => {
                                return Ok(Some(Message::AddTorrent(torrent_type)));
                            }
                            Err(_err) => todo!("handle wrong input"),
                        },
                        event::KeyCode::Esc => return Ok(Some(Message::GoToMainPage)),
                        _ => (),
                    }
                    return Ok(handle_key(key));
                }
            }
        }
    }
    Ok(None)
}

fn handle_key(key: event::KeyEvent) -> Option<Message> {
    match key.code {
        event::KeyCode::Char('q') | event::KeyCode::Esc => Some(Message::Quit),
        event::KeyCode::Char('+') => Some(Message::InitAddTorrent),
        _ => None,
    }
}

pub(crate) async fn update(model: &mut Model, msg: Message) {
    match msg {
        Message::GoToMainPage => model.go_to_main_page(),
        Message::ApplicationEvent(_application_event) => (),
        Message::Quit => model.running = false,

        Message::InitAddTorrent => {
            model.page = crate::model::NavPage::AddingTorrent(Input::new("".to_string()))
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
        }
    }
}
