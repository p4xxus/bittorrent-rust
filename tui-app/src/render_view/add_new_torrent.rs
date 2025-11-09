use ratatui::{
    Frame,
    layout::{Constraint, Position},
    style::Stylize,
    widgets::{Block, Clear, Paragraph, Wrap},
};
use tui_input::Input;

use crate::{model::Model, render_view::common::center};

pub(super) fn render_add_torrent_popup(_: &Model, frame: &mut Frame, input: &Input) {
    let area = center(
        frame.area(),
        Constraint::Percentage(50),
        Constraint::Percentage(20), // top and bottom border + content
    );
    // 1. Calculate the cursor's visual position.
    // tui-input gives you the offset in the *text value* (for horizontal scrolling)
    let text_len = input.visual_cursor() as u16;

    // 2. Determine the *physical* screen coordinates.
    // This is the starting X of the widget + the length of the visible text before the cursor.
    // The '+ 1' is often needed to account for the border on the left if using `Borders::ALL`.
    let line_index = text_len.checked_div(area.width).unwrap_or(0);
    let cursor_x = (area.x + 1 + text_len) - (line_index * (area.width - 2)); // -2 because of the borders again
    let cursor_y = area.y + 1 + line_index; // Assuming a single-line input with Borders::ALL
    let cursor_position = Position::new(cursor_x, cursor_y);
    let popup = Paragraph::new(input.value())
        .wrap(Wrap { trim: false })
        .block(
            Block::bordered()
                .border_type(ratatui::widgets::BorderType::Rounded)
                .title("Enter path/link.")
                .green(),
        );
    frame.render_widget(Clear, area);
    frame.render_widget(popup, area);
    frame.set_cursor_position(cursor_position);
}
