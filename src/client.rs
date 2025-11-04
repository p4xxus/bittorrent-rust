use std::{
    collections::HashSet,
    error::Error,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    database::SurrealDbConn,
    magnet_links::MagnetLink,
    peer_manager::PeerManager,
    torrent::{AnnounceList, InfoHash, Torrent},
};

pub(crate) const PEER_ID: &[u8; 20] = b"-AZ2060-222222222222";

const DATABASE_NAME: &'static str = "files";

#[derive(Debug)]
pub struct Client {
    db_conn: Arc<SurrealDbConn>,
    peer_managers: Arc<Mutex<HashSet<InfoHash>>>,
    options: ClientOptions,
}

#[derive(Clone, Copy, Debug)]
pub struct ClientOptions {
    pub(crate) port: u16,
    pub(crate) ip_addr: IpAddr,
}

impl From<ClientOptions> for SocketAddr {
    fn from(value: ClientOptions) -> Self {
        SocketAddr::new(value.ip_addr, value.port)
    }
}

impl Default for ClientOptions {
    /// - the default ip address is LOCALHOST
    /// - the default port is the first one thats free between 6881..=6889
    fn default() -> Self {
        let ip_addr = Ipv4Addr::LOCALHOST.into();
        // Common behavior is for a downloader to try to listen on port 6881 and if that
        // port is taken try 6882, then 6883, etc. and give up after 6889
        let mut free_port = None;
        for port in 6881..=6889 {
            let ip = SocketAddr::new(ip_addr, port);
            if TcpListener::bind(ip).is_ok() {
                free_port = Some(port);
                break;
            }
        }

        Self {
            port: free_port.expect("No free port was found."),
            ip_addr,
        }
    }
}

impl ClientOptions {
    pub async fn build(self) -> Result<Client, Box<dyn Error>> {
        if !self.is_port_free() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::AddrInUse,
                "Port is already in use.",
            ))); // TODO: ClientError
        }

        let client = Client::new(self)?;
        client.add_unfinished_torrents_from_db().await;

        Ok(client)
    }

    fn is_port_free(&self) -> bool {
        TcpListener::bind(SocketAddr::from(*self)).is_ok()
    }
}

impl Client {
    fn new(options: ClientOptions) -> Result<Self, Box<dyn Error>> {
        let db_conn = Arc::new(tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { SurrealDbConn::new(DATABASE_NAME).await })
        })?);

        Ok(Self {
            db_conn,
            peer_managers: Arc::new(Mutex::new(HashSet::new())),
            options,
        })
    }

    async fn add_unfinished_torrents_from_db(&self) {
        let entries = self
            .db_conn
            .get_all()
            .await
            .into_iter()
            .filter(|e| !e.is_finished());

        for peer_manager in entries
            .filter_map(|entry| PeerManager::init_from_entry(Arc::clone(&self.db_conn), entry).ok())
        {
            self.start_peer_manager(peer_manager);
        }
    }

    pub async fn add_torrent(
        &mut self,
        torrent_path: &PathBuf,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let torrent = Torrent::read_from_file(torrent_path)?;
        self.start_download_torrent(torrent, output_path).await?;
        Ok(())
    }

    pub async fn add_magnet(
        &self,
        magnet_link_str: &str,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let magnet_link = MagnetLink::from_url(magnet_link_str)?;
        self.start_download_magnet(magnet_link, output_path).await?;
        Ok(())
    }

    fn is_already_downloading(&self, info_hash: &InfoHash) -> bool {
        self.peer_managers.lock().unwrap().contains(info_hash)
    }

    async fn start_download_magnet(
        &self,
        magnet_link: MagnetLink,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let info_hash = magnet_link.info_hash;
        if self.is_already_downloading(&info_hash) {
            return Ok(());
        }
        let peer_manager = match self.db_conn.get_entry(&info_hash.as_hex()).await? {
            Some(file_entry) => {
                PeerManager::init_from_entry(Arc::clone(&self.db_conn), file_entry)?
            }
            None => {
                PeerManager::init_from_magnet(magnet_link, Arc::clone(&self.db_conn), output_path)
            }
        };

        self.start_peer_manager(peer_manager);

        Ok(())
    }

    async fn start_download_torrent(
        &self,
        torrent: Torrent,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let info_hash = torrent.info.info_hash();
        if self.is_already_downloading(&info_hash) {
            return Ok(());
        }
        let file_entry = match self.db_conn.get_entry(&info_hash.as_hex()).await? {
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
        let peer_manager = PeerManager::init_from_entry(Arc::clone(&self.db_conn), file_entry)?;

        self.start_peer_manager(peer_manager);

        Ok(())
    }

    fn start_peer_manager(&self, peer_manager: PeerManager) {
        let info_hash = peer_manager.get_info_hash();
        println!("Now down-/uploading file with hash {info_hash}.");
        let peer_managers = Arc::clone(&self.peer_managers);
        let client_options = self.options;
        tokio::spawn(async move {
            peer_managers.lock().unwrap().insert(info_hash);
            if let Err(err) = peer_manager.run(client_options).await {
                println!("The peer manager responsible for the hash {info_hash} failed.");
                println!("REASON:");
                println!("{err}");
                peer_managers.lock().unwrap().remove(&info_hash);
            }
        });
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
