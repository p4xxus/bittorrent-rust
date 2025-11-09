use std::time::Duration;

use ratatui::crossterm::event::{self, Event};
use tui_input::backend::crossterm::EventHandler;

use crate::model::{Message, Model, TorrentType};

/// I need mutable access to the model to update the input field
pub fn handle_user_input(model: &mut Model) -> color_eyre::Result<Option<Message>> {
    if event::poll(Duration::from_millis(100))? {
        let event = event::read()?;
        match &mut model.current_page {
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
