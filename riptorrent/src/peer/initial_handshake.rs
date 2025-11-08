use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{peer::error::PeerError, torrent::InfoHash};

#[derive(Debug, Copy, Clone, bincode::Encode, bincode::Decode)]
pub struct Handshake {
    length: u8,
    protocol: [u8; 19],
    reserved: [u8; 8],
    pub info_hash: InfoHash,
    pub peer_id: [u8; 20],
}
const HANDSHAKE_LEN: usize = std::mem::size_of::<Handshake>();

impl Handshake {
    pub fn new(info_hash: InfoHash, peer_id: [u8; 20]) -> Self {
        let mut reserved = [0_u8; 8];

        // The bit selected for the extension protocol is bit 20 from the right
        // .... 00010000 00000000 00000000
        //         ^ 20th bit from the right, counting starts at 0
        reserved[5] = 0x10;
        Self {
            length: 19,
            protocol: *b"BitTorrent protocol",
            reserved,
            info_hash,
            peer_id,
        }
    }

    /// reads the handshake from the other side and writes ours back
    pub(crate) async fn retrieve_new_connection(
        tcp: &mut tokio::net::TcpStream,
    ) -> Result<Self, PeerError> {
        let config = get_bincode_config();
        // read handshake
        let handshake_recv = read_bytes(tcp, config).await?;
        // write handshake
        write_bytes(tcp, handshake_recv, config).await?;

        Ok(handshake_recv)
    }

    /// Initializes the handshake by writing the handshake to the tcp stream
    /// and returning the handshake received from the tcp stream
    pub async fn init_new_connection(
        self,
        tcp: &mut tokio::net::TcpStream,
    ) -> Result<Handshake, PeerError> {
        let config = get_bincode_config();
        // write handshake
        write_bytes(tcp, self, config).await?;
        // read handshake
        let handshake_recv = read_bytes(tcp, config).await?;
        if handshake_recv.info_hash != self.info_hash {
            return Err(PeerError::InfoHashMismatch);
        }

        Ok(handshake_recv)
    }

    pub fn has_extensions_enabled(&self) -> bool {
        let extension_bit = self.reserved[5] & 0x10;
        extension_bit == 0x10
    }
}

fn get_bincode_config() -> impl bincode::config::Config {
    bincode::config::standard()
        .with_big_endian()
        .with_limit::<HANDSHAKE_LEN>()
}

/// write a handshake into a tcp stream
async fn write_bytes(
    tcp: &mut tokio::net::TcpStream,
    handshake: Handshake,
    config: impl bincode::config::Config,
) -> Result<(), PeerError> {
    let mut handshake_bytes = [0_u8; HANDSHAKE_LEN];
    bincode::encode_into_slice(handshake, &mut handshake_bytes, config)?;
    tcp.write_all(&handshake_bytes)
        .await
        .map_err(|error| PeerError::SendToPeer {
            error,
            peer_id: handshake.peer_id,
            msg_type_str: "Handshake".to_string(),
        })?;

    Ok(())
}

async fn read_bytes(
    tcp: &mut tokio::net::TcpStream,
    config: impl bincode::config::Config,
) -> Result<Handshake, PeerError> {
    let mut handshake_bytes = [0_u8; HANDSHAKE_LEN];
    tcp.read_exact(&mut handshake_bytes)
        .await
        .map_err(PeerError::RecvHandshake)?;

    let (handshake_recv, len) =
        bincode::decode_from_slice::<Handshake, _>(&handshake_bytes, config)?;

    assert_eq!(len, HANDSHAKE_LEN);
    assert_eq!(handshake_recv.length, 19);
    assert_eq!(handshake_recv.protocol, *b"BitTorrent protocol");

    Ok(handshake_recv)
}
