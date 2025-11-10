use std::borrow::Cow;

use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
};
use tui_logger::{LogFormatter, TuiLoggerWidget};

use crate::model::Model;

static FORMATTER: CustomLogFormat = CustomLogFormat {
    show_timestamp: true,
};

pub(super) fn render_logger(_: &Model, frame: &mut Frame, viewport: Rect) {
    let tracing_widget = TuiLoggerWidget::default().formatter(Box::new(FORMATTER)); // not really nice but who cares at this point
    frame.render_widget(tracing_widget, viewport);
}

#[derive(Clone, Copy)]
struct CustomLogFormat {
    show_timestamp: bool,
}

impl LogFormatter for CustomLogFormat {
    fn min_width(&self) -> u16 {
        20
    }

    fn format(
        &self,
        _width: usize,
        evt: &tui_logger::ExtLogRecord,
    ) -> Vec<ratatui::prelude::Line<'_>> {
        let mut lines = Vec::new();

        // Choose style based on log level
        let style = match evt.level {
            log::Level::Error => Style::default().fg(Color::Red),
            log::Level::Warn => Style::default().fg(Color::Yellow),
            log::Level::Info => Style::default().fg(Color::Cyan),
            _ => Style::default(),
        };

        // Build the message string
        let mut output = String::new();
        if self.show_timestamp {
            output.push_str(&format!("{} ", evt.timestamp.format("%H:%M:%S")));
        }
        output.push_str(evt.msg());

        // Create a Line with styled Span
        let span = Span {
            style,
            content: Cow::Owned(output),
        };
        lines.push(Line::from(vec![span]));

        lines
    }
}
