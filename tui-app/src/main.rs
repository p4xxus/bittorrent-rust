use std::{io::Stdout, time::Instant};

use ratatui::{Terminal, prelude::CrosstermBackend};
use riptorrent::ClientOptions;
use tachyonfx::{EffectTimer, Interpolation, fx};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{
    model::Model,
    render_view::render_view,
    update::{torrent_event::handle_client_event, update, user_input::handle_user_input},
};

mod model;
mod render_view;
mod update;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let mut terminal = ratatui::init();
    tracing_subscriber::registry()
        .with(tui_logger::TuiTracingSubscriberLayer)
        .init();
    tui_logger::init_logger(tui_logger::LevelFilter::Warn)?;
    // tui_logger::set_env_filter_from_string("riptorrent");

    let client = ClientOptions::default()
        .with_continue_download(false)
        .build()
        .await
        .expect("Failed to initialize the client.");

    let mut events_rx = client.subscribe_to_events();
    let torrents = client.get_all_torrents().await;
    let mut model = Model::new(client, torrents);

    while model.running {
        terminal.draw(|f| {
            render_view(&mut model, f);
        })?;

        if let Some(msg) = handle_client_event(&model, &mut events_rx) {
            update(&mut model, msg).await;
        }
        if let Some(msg) = handle_user_input(&mut model)? {
            update(&mut model, msg).await;
        }
    }

    // explode(&mut terminal, model)?;
    ratatui::restore();

    Ok(())
}

fn explode(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    mut model: Model,
) -> color_eyre::Result<()> {
    let timer = EffectTimer::from_ms(1000, Interpolation::QuadInOut);
    let mut explode_effect =
        fx::explode(5.0, 4.0, timer).with_color_space(tachyonfx::ColorSpace::Rgb);

    let mut last_frame = Instant::now();
    while !explode_effect.done() {
        let elapsed = last_frame.elapsed();
        last_frame = Instant::now();
        terminal.draw(|f| {
            render_view(&mut model, f);
            // Apply effects
            let area = f.area();
            explode_effect.process(elapsed.into(), f.buffer_mut(), area);
        })?;
    }
    Ok(())
}
