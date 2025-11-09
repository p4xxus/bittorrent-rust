use ratatui::{
    Frame,
    layout::{Constraint, Layout},
    style::{Modifier, Style},
    text::Text,
    widgets::Gauge,
};

use crate::{
    model::{Model, TorrentInfo},
    render_view::common::center,
};

pub(super) fn render_torrent_list(model: &Model, frame: &mut Frame) {
    let row_layout = Layout::vertical(vec![Constraint::Length(5); model.torrents.values().count()])
        .margin(2)
        .split(frame.area());

    for (index, torrent_info) in model.torrents.values().enumerate() {
        let area = row_layout[index];
        let column_layout = Layout::horizontal(Constraint::from_fills([1, 2]));
        let [paragraph_area, gauge_area] = column_layout.areas(area);

        let mut paragraph = Text::default();
        paragraph.push_span(torrent_info.file_path.display().to_string());
        let ratio = calculate_torrent_ratio(torrent_info);
        let gauge = Gauge::default()
            .ratio(ratio)
            .label((ratio * 100.0).to_string())
            .style(Style::default().add_modifier(Modifier::DIM));

        let paragraph_area = center(
            paragraph_area,
            Constraint::Length(paragraph.width() as u16),
            Constraint::Length(paragraph.lines.iter().count() as u16),
        );
        frame.render_widget(paragraph, paragraph_area);
        frame.render_widget(gauge, gauge_area);
    }
}

/// calculates the ratio for the gauge how much we've finished
fn calculate_torrent_ratio(torrent_info: &TorrentInfo) -> f64 {
    torrent_info.bitfield.iter().filter(|&b| *b).count() as f64 / torrent_info.number_pieces as f64
}
