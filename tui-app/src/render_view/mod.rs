use ratatui::{
    Frame,
    layout::{Constraint, Flex, Layout, Position, Rect},
    style::{Style, Stylize},
    widgets::{Block, Borders, Clear, List, ListState, Paragraph},
};
use tui_input::Input;

use crate::model::Model;

pub fn view(model: &Model, frame: &mut Frame) {
    match &model.page {
        crate::model::NavPage::TorrentList => render_torrent_list(model, frame),
        crate::model::NavPage::AddingTorrent(input) => {
            render_add_torrent_popup(model, frame, input)
        }
    }
    //... use `ratatui` functions to draw your UI based on the model's state
}

fn render_torrent_list(model: &Model, frame: &mut Frame) {
    let mut state = ListState::default().with_selected(Some(0));
    let list = List::new(
        model
            .torrents
            .iter()
            .map(|torrent| torrent.file_path.to_str().expect("valid utf8").to_string())
            .collect::<Vec<String>>(),
    )
    .block(Block::bordered().border_type(ratatui::widgets::BorderType::Rounded))
    .style(Style::new().white())
    .highlight_style(Style::new().red())
    .highlight_symbol(">");

    frame.render_stateful_widget(list, frame.area(), &mut state);
}

fn render_add_torrent_popup(_: &Model, frame: &mut Frame, input: &Input) {
    let area = center(
        frame.area(),
        Constraint::Percentage(20),
        Constraint::Length(3), // top and bottom border + content
    );
    // 1. Calculate the cursor's visual position.
    // tui-input gives you the offset in the *text value* (for horizontal scrolling)
    let text_len = input.visual_cursor();

    // 2. Determine the *physical* screen coordinates.
    // This is the starting X of the widget + the length of the visible text before the cursor.
    // The '+ 1' is often needed to account for the border on the left if using `Borders::ALL`.
    let cursor_x = area.x + 1 + text_len as u16;
    let cursor_y = area.y + 1; // Assuming a single-line input with Borders::ALL
    let cursor_position = Position::new(cursor_x, cursor_y);
    let popup = Paragraph::new(input.value()).block(Block::bordered().title("Enter path/link."));
    frame.render_widget(Clear, area);
    frame.render_widget(popup, area);
    frame.set_cursor_position(cursor_position);
}

fn center(area: Rect, horizontal: Constraint, vertical: Constraint) -> Rect {
    let [area] = Layout::horizontal([horizontal])
        .flex(Flex::Center)
        .areas(area);
    let [area] = Layout::vertical([vertical]).flex(Flex::Center).areas(area);
    area
}
