use riptorrent::ClientOptions;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    model::Model,
    render_view::view,
    update::{torrent_event::handle_client_event, update, user_input::handle_user_input},
};

mod model;
mod render_view;
mod update;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let mut terminal = ratatui::init();
    tui_logger::init_logger(tui_logger::LevelFilter::Info)?;
    tui_logger::set_env_filter_from_string("riptorrent");
    tracing_subscriber::registry()
        .with(tui_logger::TuiTracingSubscriberLayer)
        .init();

    let client = ClientOptions::default()
        .build()
        .await
        .expect("Failed to initialize the client.");

    let mut events_rx = client.subscribe_to_events();
    let torrents = client.get_all_torrents().await;
    let mut model = Model::new(client, torrents);

    while model.running {
        terminal.draw(|f| view(&mut model, f))?;

        if let Some(msg) = handle_client_event(&model, &mut events_rx) {
            update(&mut model, msg).await;
        }
        if let Some(msg) = handle_user_input(&mut model)? {
            update(&mut model, msg).await;
        }
    }

    ratatui::restore();

    Ok(())
}
