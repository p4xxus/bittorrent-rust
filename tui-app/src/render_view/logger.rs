use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Style},
};
use tui_logger::TuiLoggerWidget;

use crate::model::Model;

pub(super) fn render_logger(_: &Model, frame: &mut Frame, viewport: Rect) {
    let tracing_widget = TuiLoggerWidget::default()
        .style_error(Style::default().fg(Color::Red))
        .style_info(Style::default().fg(Color::Cyan))
        .style_warn(Style::default().fg(Color::Yellow));
    frame.render_widget(tracing_widget, viewport);
}
