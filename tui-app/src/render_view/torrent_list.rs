use ratatui::{
    Frame,
    layout::{Constraint, Flex, Layout, Rect, Spacing},
    style::{Color, Modifier, Style, Stylize},
    text::Span,
    widgets::{Gauge, Paragraph, Wrap},
};

use crate::model::{Model, TorrentInfo};

pub(super) fn render_torrent_list(model: &Model, frame: &mut Frame, viewport: Rect) {
    let row_layout = Layout::vertical(vec![Constraint::Length(5); model.torrents.values().count()])
        .margin(2)
        .spacing(Spacing::Space(1))
        .split(viewport);

    for (index, torrent_info) in model.torrents.values().enumerate() {
        let area = row_layout[index];
        let column_layout = Layout::horizontal(Constraint::from_fills([1, 2]));
        let [paragraph_area, gauge_area] = column_layout.areas(area);

        render_gauge(frame, torrent_info, gauge_area);

        render_torrent_information(frame, torrent_info, paragraph_area);
    }
}

fn render_gauge(frame: &mut Frame, torrent_info: &TorrentInfo, area: Rect) {
    let ratio = calculate_torrent_ratio(torrent_info);
    let gauge = Gauge::default()
        .ratio(ratio)
        .label((ratio * 100.0).to_string())
        .add_modifier(Modifier::DIM)
        .gauge_style(value_to_color(ratio));

    frame.render_widget(gauge, area);
}

fn render_torrent_information(frame: &mut Frame, torrent_info: &TorrentInfo, area: Rect) {
    let [torrent_name_area, peer_connections_area] =
        Layout::horizontal([Constraint::Fill(4), Constraint::Fill(1)])
            .flex(Flex::SpaceBetween)
            .areas(area);

    //      name
    let paragraph =
        Paragraph::new(torrent_info.file_path.display().to_string()).wrap(Wrap { trim: true });
    frame.render_widget(paragraph, torrent_name_area);

    //      peer connections
    let [inbound_area, outbound_area] = Layout::vertical(vec![Constraint::Fill(1); 2])
        .flex(Flex::Center)
        .margin(1)
        .areas(peer_connections_area);

    let inbound_text = Span::styled(
        format!("{} ↓", torrent_info.peer_connections.inbound()),
        Style::new().green(),
    );
    frame.render_widget(inbound_text, inbound_area);
    let outbound_text = Span::styled(
        format!("{} ↑", torrent_info.peer_connections.outbound()),
        Style::new().blue(),
    );
    frame.render_widget(outbound_text, outbound_area);
}

/// calculates the ratio for the gauge how much we've finished
fn calculate_torrent_ratio(torrent_info: &TorrentInfo) -> f64 {
    torrent_info.bitfield.iter().filter(|&b| *b).count() as f64 / torrent_info.num_pieces() as f64
}

/// Maps a value from 0.0 to 1.0 to an RGB color.
///
/// This gradient goes from Red (at 0.0) -> Yellow (at 0.5) -> Green (at 1.0).
fn value_to_color(value: f64) -> Color {
    // Clamp the value to ensure it's in the [0.0, 1.0] range
    let v = value.clamp(0.0, 1.0);

    let r: u8;
    let g: u8;
    let b: u8 = 0; // Blue is always 0 in this specific gradient

    if v < 0.5 {
        // Phase 1: Red to Yellow
        // As `v` goes from 0.0 to 0.5, `g` goes from 0 to 255
        let phase_value = v * 2.0; // Scale 0.0-0.5 to 0.0-1.0
        r = 255;
        g = (phase_value * 255.0).round() as u8;
    } else {
        // Phase 2: Yellow to Green
        // As `v` goes from 0.5 to 1.0, `r` goes from 255 to 0
        let phase_value = (v - 0.5) * 2.0; // Scale 0.5-1.0 to 0.0-1.0
        r = (255.0 * (1.0 - phase_value)).round() as u8;
        g = 255;
    }

    Color::Rgb(r, g, b)
}
