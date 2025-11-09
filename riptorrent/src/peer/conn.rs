//! Contains methods to construct all the connections and types relevant for the peer:
//! Setting up TCP, constructing a stream of the remote peer & the peer manager.
//!
//! Also contains types that store the state of the peer (interested/choking, ...)

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

use futures_core::Stream;
use futures_util::SinkExt;
use futures_util::StreamExt;
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use futures_util::stream::unfold;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio_util::codec::Framed;
use tokio_util::time::FutureExt;

use crate::events::ApplicationEvent;
use crate::events::PeerEvent;
use crate::events::emit_event;
use crate::messages::{MessageFramer, PeerMessage};
use crate::peer::DEFAULT_MAX_REQUESTS;
use crate::peer::Msg;
use crate::peer::Peer;
use crate::peer::error::PeerError;
use crate::peer::initial_handshake::Handshake;
use crate::peer_manager::PeerConn;
use crate::peer_manager::ReqMessage;
use crate::peer_manager::ReqMsgFromPeer;
use crate::peer_manager::ResMessage;
use crate::torrent::InfoHash;

const CHANNEL_SIZE: usize = 16;
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(60);

impl Peer {
    /// Constructs a peer from an address
    pub async fn connect_from_addr(
        addr: SocketAddr,
        info_hash: InfoHash,
        peer_id: [u8; 20],
        peer_manager_tx: Sender<ReqMsgFromPeer>,
    ) -> Result<Self, PeerError> {
        // set up tcp connection & shake hands
        let mut tcp = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|error| PeerError::FailedToConnect { error, addr })?;

        let handshake_recv = Handshake::new(info_hash, peer_id)
            .init_new_connection(&mut tcp)
            .await?;

        Peer::new_from_stream(tcp, handshake_recv, peer_manager_tx).await
    }

    /// Constructs a peer from an already existing connection.
    /// The hands must already be shook in order to assure that the info-hash corresponds to the correct peer-manager
    ///
    /// This happens when its an incoming connection (for seeding).
    pub(crate) async fn new_from_stream(
        tcp: TcpStream,
        handshake_recv: Handshake,
        peer_manager_tx: Sender<ReqMsgFromPeer>,
    ) -> Result<Self, PeerError> {
        let peer_state = PeerState::new(handshake_recv.peer_id);

        // after the handshake as succeeded we can create the message framer that de- & encodes the messages
        // from the tcp stream
        let framed = Framed::new(tcp, MessageFramer);

        // set up peer_manager connection
        let peer_manager_rx = peer_state.connect_to_peer_manager(&peer_manager_tx).await?;
        let (peer_writer, peer_reader) = framed.split();
        let receiver_stream = Some(construct_stream(peer_reader, peer_manager_rx).await);

        // set up extensions
        let extensions = handshake_recv
            .has_extensions_enabled()
            .then_some(HashMap::new());

        Ok(Self {
            state: peer_state,
            queue: crate::peer::ReqQueue::new(),
            peer_manager_tx,
            peer_writer,
            receiver_stream,
            extensions: Mutex::new(extensions),
        })
    }
}

/// Creates the stream consisting of
/// - The receiver of the remote peer itself
/// - The receiver of the peer manager
async fn construct_stream(
    framed_rx: PeerReader,
    peer_manager_rx: Receiver<ResMessage>,
) -> BoxedMsgStream {
    let peer_msg_stream = unfold(framed_rx, |mut framed| async move {
        match framed.next().timeout(KEEPALIVE_TIMEOUT).await {
            Ok(Some(Ok(message))) => Some((Msg::Data(message), framed)),
            Err(_) => Some((Msg::Timeout, framed)),
            Ok(None) => {
                // nothing really happens here
                // we haven't received all the data yet
                None
            }
            Ok(Some(Err(e))) => Some((Msg::CloseConnection(e.kind()), framed)),
        }
    });

    // this is the stream sent by other connections to peers to send have messages
    let manager_stream =
        tokio_stream::wrappers::ReceiverStream::new(peer_manager_rx).map(Msg::Manager);

    let stream = tokio_stream::StreamExt::merge(peer_msg_stream, manager_stream);
    Box::pin(stream)
}

/// wrapper function that maps the result of the sending to our error
pub(super) async fn send_peer_manager(
    peer_manager_tx: &Sender<ReqMsgFromPeer>,
    msg: ReqMsgFromPeer,
    peer_id: [u8; 20],
) -> Result<(), PeerError> {
    peer_manager_tx
        .send(msg)
        .await
        .map_err(|error| PeerError::SendToPeerManager { error, peer_id })
}

/// this is just a wrapper type for the actual states that wraps it in an Arc to be able to clone them + Send
#[derive(Debug, Clone)]
pub(crate) struct PeerState(pub(crate) Arc<PeerStateInner>);

/// This keeps track of the peer_id, the choking & interested state of both sides, and whats the `reqq` (max request) field in the extension handshake
#[derive(Debug)]
pub(crate) struct PeerStateInner {
    /// the peer_id of the remote peer
    pub(crate) peer_id: [u8; 20],
    // dk if I need this at all
    // pub state: Arc<Mutex<super::PeerState>>,
    pub(crate) am_choking: AtomicBool,
    pub(crate) am_interested: AtomicBool,
    pub(crate) peer_choking: AtomicBool,
    pub(crate) peer_interested: AtomicBool,
    pub(crate) max_req: AtomicU32,
}

impl PeerState {
    pub(crate) fn new(peer_id: [u8; 20]) -> Self {
        let peer_identifier_inner = PeerStateInner {
            peer_id,
            am_choking: AtomicBool::new(true),
            am_interested: AtomicBool::new(false),
            peer_choking: AtomicBool::new(true),
            peer_interested: AtomicBool::new(false),
            max_req: AtomicU32::new(DEFAULT_MAX_REQUESTS),
        };
        Self(Arc::new(peer_identifier_inner))
    }

    async fn connect_to_peer_manager(
        &self,
        peer_manager_tx: &Sender<ReqMsgFromPeer>,
    ) -> Result<Receiver<ResMessage>, PeerError> {
        let (sender, peer_manager_rx) = mpsc::channel(CHANNEL_SIZE);
        let peer_conn = PeerConn {
            sender,
            identifier: self.clone(),
        };
        let peer_id = self.0.peer_id;
        let msg = ReqMsgFromPeer {
            peer_id,
            msg: ReqMessage::NewConnection(peer_conn),
        };
        send_peer_manager(peer_manager_tx, msg, peer_id).await?;

        Ok(peer_manager_rx)
    }
}

pub(in crate::peer) fn emit_peer_event(peer_event: PeerEvent, info_hash: InfoHash) {
    emit_event(ApplicationEvent::Peer(peer_event, info_hash));
}

pub(super) type BoxedMsgStream = Pin<Box<dyn Stream<Item = Msg> + Send + Sync>>;
pub(super) type PeerWriter = SplitSink<Framed<TcpStream, MessageFramer>, PeerMessage>;
type PeerReader = SplitStream<Framed<TcpStream, MessageFramer>>;

impl Drop for Peer {
    fn drop(&mut self) {
        // send message to peer manager that so he removes us
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let _ = self.set_interested(false).await;
                let _ = self
                    .send_peer_manager(ReqMessage::PeerDisconnected(InfoHash(self.get_id())))
                    .await;
                let _ = self.peer_writer.close().await;
            });
        });
    }
}
