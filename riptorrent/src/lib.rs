mod extensions;
mod messages;
mod peer;
mod peer_manager;

pub mod client;
pub mod core;
pub mod database;
pub mod events;

pub use client::ClientOptions;
pub use core::torrent;
pub use core::tracker::TrackerRequest;
pub use extensions::magnet_links;
pub use peer::Peer;
pub use peer_manager::PeerManager;

pub(crate) const BLOCK_MAX: u32 = 1 << 14;
