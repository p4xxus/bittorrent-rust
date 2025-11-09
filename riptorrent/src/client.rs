use std::{
    collections::HashMap,
    error::Error,
    fmt::Display,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use tokio::{
    net::TcpListener as TokioTcpListener,
    sync::{broadcast, mpsc},
};

use crate::{
    database::{FileInfo, SurrealDbConn},
    events::{ApplicationEvent, PeerEvent, emit_event, get_receiver},
    magnet_links::MagnetLink,
    peer::initial_handshake::Handshake,
    peer_manager::{PeerManager, ReqMsgFromPeer},
    torrent::{AnnounceList, InfoHash, Torrent},
};

pub(crate) const PEER_ID: &[u8; 20] = b"-AZ2060-222222222222";

const DATABASE_NAME: &str = "files";

#[derive(Debug, Clone)]
pub struct Client {
    db_conn: Arc<SurrealDbConn>,
    peer_managers: Arc<Mutex<HashMap<InfoHash, mpsc::Sender<ReqMsgFromPeer>>>>,
    options: ClientOptions,
}

/// A struct that represents all the possible options for a session (client).
/// With this you can create a client via the [`build`] method
///
/// # Usage
/// ```rust
/// # use std::error::Error;
/// # use std::path::PathBuf;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
/// use riptorrent::ClientOptions;
/// let client = ClientOptions::default()
///     .with_continue_download(false)
///     .build()
///     .await?;
///
/// // You have to `.await` them in order to start download, this is only a doctest, not a bittorrent client
/// client.add_magnet("some valid magnet link", Some(PathBuf::from("output/path.txt")));
/// client.add_torrent(&PathBuf::from("torrent_path"), None);
/// #   Ok(())
/// # }
/// ```
///
#[derive(Clone, Copy, Debug)]
pub struct ClientOptions {
    /// port to listen on
    pub(crate) port: u16,
    /// ip address to listen on
    pub(crate) ip_addr: IpAddr,
    /// whether to continue downloading previous files
    continue_download: bool,
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
            continue_download: true,
        }
    }
}

impl ClientOptions {
    pub async fn build(self) -> Result<Client, Box<dyn Error + Send + Sync + 'static>> {
        if !self.is_port_free() {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::AddrInUse,
                "Port is already in use.",
            ))); // TODO: ClientError
        }

        let client = Client::new(self)?;

        if self.continue_download {
            client.continue_download_unfinished().await;
        }

        client.spawn_listener()?;

        Ok(client)
    }

    fn is_port_free(&self) -> bool {
        TcpListener::bind(SocketAddr::from(*self)).is_ok()
    }

    pub fn with_continue_download(mut self, continue_download: bool) -> Self {
        self.continue_download = continue_download;
        self
    }
}

impl Client {
    /// Starts downloading a torrent from a path to the torrent file.
    /// If no output path was provided, it will use the one found in the torrent file.
    pub async fn add_torrent(
        &self,
        torrent_path: &PathBuf,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let torrent = Torrent::read_from_file(torrent_path)?;
        self.start_download_torrent(torrent, output_path).await?;
        Ok(())
    }

    /// Starts downloading a torrent from a valid magnet link.
    /// If no output path was provided, it will use the one found in the torrent file.
    pub async fn add_magnet(
        &self,
        magnet_link_str: &str,
        output_path: Option<PathBuf>,
    ) -> Result<(), Box<dyn Error>> {
        let magnet_link = MagnetLink::from_url(magnet_link_str)?;
        self.start_download_magnet(magnet_link, output_path).await?;
        Ok(())
    }

    pub fn subscribe_to_events(&self) -> broadcast::Receiver<ApplicationEvent> {
        get_receiver()
    }

    /// Looks into the db, which files are not finished yet and well finishes them.
    pub async fn continue_download_unfinished(&self) {
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

    pub async fn get_all_torrents(&self) -> Vec<FileInfo> {
        self.db_conn
            .get_all()
            .await
            .into_iter()
            .map(Into::into)
            .collect()
    }

    fn is_already_downloading(&self, info_hash: &InfoHash) -> bool {
        self.peer_managers.lock().unwrap().contains_key(info_hash)
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

    fn new(options: ClientOptions) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let db_conn = Arc::new(tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async move { SurrealDbConn::new(DATABASE_NAME).await })
        })?);

        Ok(Self {
            db_conn,
            peer_managers: Arc::new(Mutex::new(HashMap::new())),
            options,
        })
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
        let info_hash = peer_manager.info_hash;
        let peer_managers = Arc::clone(&self.peer_managers);
        let client_options = self.options;
        tokio::spawn(async move {
            if let Err(err) = peer_manager.run(client_options).await {
                emit_event(ApplicationEvent::Torrent(
                    crate::events::TorrentEvent::DownloadCanceled(Arc::new(err)),
                    info_hash,
                ));
                peer_managers.lock().unwrap().remove(&info_hash);
                // TODO: remove from db??
            }
        });
    }

    fn spawn_listener(&self) -> io::Result<()> {
        let client = self.clone();
        let socket_addr = SocketAddr::from(self.options);
        let tcp_stream = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { TokioTcpListener::bind(socket_addr).await })
        })?;

        tokio::spawn(async move {
            while let Ok((mut tcp_stream, _addr)) = tcp_stream.accept().await {
                if let Ok(handshake_recv) =
                    Handshake::retrieve_new_connection(&mut tcp_stream).await
                {
                    let peer_manager_tx = client
                        .peer_managers
                        .lock()
                        .unwrap()
                        .get(&handshake_recv.info_hash)
                        .cloned();

                    let get_peer = async || {
                        return match peer_manager_tx {
                            // first, do we already have a peer_manager running for this torrent?
                            Some(peer_manager_tx) => crate::Peer::new_from_stream(
                                tcp_stream,
                                handshake_recv,
                                peer_manager_tx.clone(),
                            )
                            .await
                            .map_err(stringify),

                            // No, then we might still have the file but just not a peer manager
                            None => {
                                // TODO: 2x error handling
                                if let Some(entry) = client
                                    .db_conn
                                    .get_entry(&handshake_recv.info_hash.as_hex())
                                    .await
                                    .map_err(stringify)?
                                {
                                    let peer_manager = PeerManager::init_from_entry(
                                        Arc::clone(&client.db_conn),
                                        entry,
                                    )
                                    .map_err(stringify)?;
                                    let peer_manager_tx = peer_manager.get_sender();

                                    client.start_peer_manager(peer_manager);

                                    crate::Peer::new_from_stream(
                                        tcp_stream,
                                        handshake_recv,
                                        peer_manager_tx,
                                    )
                                    .await
                                    .map_err(stringify)
                                } else {
                                    Err("A peer want something, we don't have D:<".to_string())
                                }
                            }
                        };
                    };
                    match get_peer().await {
                        Ok(peer) => {
                            emit_event(ApplicationEvent::Peer(
                                PeerEvent::NewConnectionInbound,
                                handshake_recv.info_hash,
                            ));
                            tokio::spawn(async move {
                                peer.run_gracefully(handshake_recv.info_hash).await;
                            });
                        }
                        Err(_) => (),
                    }
                }
            }
        });

        Ok(())
    }
}

/// temporary error handling
fn stringify(error: impl Display) -> String {
    format!("Got error: {error}.")
}

pub mod errors {
    use thiserror::Error;

    #[derive(Debug, Error)]

    pub enum InitError {
        #[error("No output path was provided.")]
        NoOutputPath,
    }
}
