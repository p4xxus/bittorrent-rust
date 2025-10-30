use std::{
    collections::HashMap,
    error::Error,
    io,
    net::{Ipv4Addr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Context;
use tokio::sync::mpsc;

use crate::{
    database::SurrealDbConn,
    magnet_links::MagnetLink,
    peer::Peer,
    peer_manager::{PeerManager, ReqMsgFromPeer},
    torrent::{AnnounceList, InfoHash, Torrent},
};

pub(crate) const PEER_ID: &[u8; 20] = b"-AZ2060-222222222222";

const DATABASE_NAME: &'static str = "files";

pub struct Client {
    db_conn: Arc<SurrealDbConn>,
    peer_managers: HashMap<InfoHash, PeerManager>,
}

impl Client {
    pub async fn new() -> Result<Self, Box<dyn Error>> {
        let db_conn = Arc::new(SurrealDbConn::new(DATABASE_NAME).await?);
        Ok(Self {
            db_conn,
            peer_managers: HashMap::new(),
        })
    }

    pub async fn download_torrent(
        &mut self,
        peer_port: u16,
        torrent_path: &PathBuf,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let torrent = Torrent::read_from_file(torrent_path)?;
        self.start_download_torrent(peer_port, torrent, output_path)
            .await?;
        Ok(())
    }

    pub async fn download_magnet(
        &mut self,
        peer_port: u16,
        magnet_link_str: &str,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let magnet_link = MagnetLink::from_url(magnet_link_str)?;
        self.start_download_magnet(peer_port, magnet_link, output_path)
            .await?;
        Ok(())
    }

    async fn start_download_magnet(
        &self,
        peer_port: u16,
        magnet_link: MagnetLink,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let info_hash = magnet_link.info_hash;

        let info_hash_hex = info_hash.as_hex();
        let peer_manager = match self.db_conn.get_entry(&info_hash_hex).await? {
            Some(file_entry) => {
                PeerManager::init_from_entry(info_hash_hex, Arc::clone(&self.db_conn), file_entry)?
            }
            None => {
                PeerManager::init_from_magnet(magnet_link, Arc::clone(&self.db_conn), output_path)
            }
        };

        tokio::spawn(async move {
            let _ = peer_manager.run().await;
        });

        // self.add_peers_to_manager(response.peers.0, info_hash, *PEER_ID, &peer_manager_tx)
        //     .await;

        // self.start_peer_listener(info_hash, peer_port, *PEER_ID, &peer_manager_tx)
        //     .await?;

        loop {
            tokio::task::yield_now().await;
        }
        println!("we're done");

        Ok(())
    }

    async fn start_download_torrent(
        &self,
        peer_port: u16,
        torrent: Torrent,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let info_hash = torrent.info.info_hash();

        let info_hash_hex = info_hash.as_hex();
        let file_entry = match self.db_conn.get_entry(&info_hash_hex).await? {
            Some(file_entry) => file_entry,
            None => {
                let announce_list = torrent
                    .announce_list
                    .unwrap_or_else(|| AnnounceList::from_single_announce(torrent.announce));
                self.db_conn
                    .set_entry(
                        output_path.unwrap_or(torrent.info.name.clone().into()),
                        torrent.info,
                        announce_list,
                    )
                    .await?
            }
        };
        let peer_manager =
            PeerManager::init_from_entry(info_hash_hex, Arc::clone(&self.db_conn), file_entry)?;

        tokio::spawn(async move {
            let _ = peer_manager.run().await;
        });

        // self.add_peers_to_manager(response.peers.0, info_hash, *PEER_ID, &peer_manager_tx)
        //     .await;

        // self.start_peer_listener(info_hash, peer_port, *PEER_ID, &peer_manager_tx)
        //     .await?;

        loop {
            tokio::task::yield_now().await;
        }
        println!("we're done");

        Ok(())
    }

    // TODO: not sure if this needs self in the future at all
    async fn start_peer_listener(
        &self,
        info_hash: InfoHash,
        peer_port: u16,
        peer_id: [u8; 20],
        peer_manager_tx: &mpsc::Sender<ReqMsgFromPeer>,
    ) -> io::Result<()> {
        let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, peer_port);
        let listener = tokio::net::TcpListener::bind(addr).await?;
        while let Ok((stream, _addr)) = listener.accept().await {
            let peer =
                Peer::connect_from_stream(stream, info_hash, peer_id, peer_manager_tx.clone())
                    .await
                    .context("initializing incoming peer connection")
                    .unwrap();
            peer.run().await.unwrap();
        }
        Ok(())
    }
}

pub mod errors {
    use thiserror::Error;

    #[derive(Debug, Error)]

    pub enum InitError {
        #[error("No output path was provided.")]
        NoOutputPath,
    }
}
