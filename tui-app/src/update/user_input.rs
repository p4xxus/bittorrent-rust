use std::time::Duration;

use ratatui::crossterm::event::{self, Event, MediaKeyCode};
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
                    Ok(match key.code {
                        event::KeyCode::Down | event::KeyCode::Char('j') => {
                            model.list_state.next();
                            None
                        }
                        event::KeyCode::Up | event::KeyCode::Char('k') => {
                            model.list_state.previous();
                            None
                        }
                        event::KeyCode::Char(' ')
                        | event::KeyCode::Pause
                        | event::KeyCode::Media(MediaKeyCode::PlayPause) => {
                            Some(Message::PauseResumeTorrent)
                        }
                        _ => handle_key_global(key),
                    })
                } else {
                    Ok(None)
                }
            }
            crate::model::NavPage::AddingTorrent(input) => {
                if input.handle_event(&event).is_none()
                    && let Event::Key(key) = event
                {
                    Ok(match key.code {
                        event::KeyCode::Enter => match TorrentType::try_from(input.value()) {
                            Ok(torrent_type) => Some(Message::AddTorrent(torrent_type)),
                            Err(_) => todo!("handle wrong input"),
                        },
                        event::KeyCode::Esc => Some(Message::GoToMainPage),
                        _ => handle_key_global(key),
                    })
                } else {
                    Ok(None)
                }
            }
        }
    } else {
        Ok(None)
    }
}

fn handle_key_global(key: event::KeyEvent) -> Option<Message> {
    match key.code {
        event::KeyCode::Char('q') | event::KeyCode::Esc => Some(Message::Quit),
        event::KeyCode::Char('+') => Some(Message::InitAddTorrent),
        _ => None,
    }
}
