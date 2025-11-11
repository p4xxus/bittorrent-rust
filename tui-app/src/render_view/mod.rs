use ratatui::{
    Frame,
    layout::{Constraint, Layout},
};

use crate::{
    model::Model,
    render_view::{
        add_new_torrent::render_add_torrent_popup, logger::render_logger,
        torrent_list::render_torrent_list,
    },
};

mod add_new_torrent;
mod common;
mod logger;
mod torrent_list;

pub fn render_view(model: &Model, frame: &mut Frame) {
    // realistically it should be one above
    let horizontal_layout = Layout::horizontal(vec![Constraint::Fill(1), Constraint::Fill(1)])
        .margin(2)
        .split(frame.area());
    let app_viewport = horizontal_layout[0];

    match &model.current_page {
        crate::model::NavPage::TorrentList => render_torrent_list(model, frame, app_viewport),
        crate::model::NavPage::AddingTorrent(input) => {
            render_add_torrent_popup(model, frame, app_viewport, input)
        }
    }

    render_logger(model, frame, horizontal_layout[1]);
}
