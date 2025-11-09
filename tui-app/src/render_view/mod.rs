use ratatui::Frame;

use crate::{
    model::Model,
    render_view::{add_new_torrent::render_add_torrent_popup, torrent_list::render_torrent_list},
};

mod add_new_torrent;
mod common;
mod torrent_list;

pub fn view(model: &Model, frame: &mut Frame) {
    match &model.current_page {
        crate::model::NavPage::TorrentList => render_torrent_list(model, frame),
        crate::model::NavPage::AddingTorrent(input) => {
            render_add_torrent_popup(model, frame, input)
        }
    }
    //... use `ratatui` functions to draw your UI based on the model's state
}
