use crate::{
    torrent::InfoHash,
    tracker::{peers::PeerConnections, peers6::Peer6Connections},
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, time::Duration};

const TRACKER_REQ_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize)]
pub struct TrackerRequest<'a> {
    /// the info hash of the torrent
    info_hash: &'a InfoHash,
    /// a unique identifier for your client
    peer_id: &'a [u8; 20],
    /// the port your client is listening on
    port: u16,
    /// the total amount uploaded so far
    uploaded: u32,
    /// the total amount downloaded so far
    downloaded: u32,
    /// the number of bytes left to download
    left: u32,
    /// whether the peer list should use the compact representation
    /// The compact representation is more commonly used in the wild, the non-compact representation is mostly supported for backward-compatibility.
    compact: u8,
}

impl<'a> TrackerRequest<'a> {
    pub fn new(info_hash: &'a InfoHash, peer_id: &'a [u8; 20], port: u16, left: u32) -> Self {
        Self {
            info_hash,
            peer_id,
            port,
            uploaded: 0,
            downloaded: 0,
            left,
            compact: 1,
        }
    }
    fn to_url_encoded(&self) -> String {
        let mut url_encoded = String::new();
        url_encoded.push_str(&format!("info_hash={}", escape_bytes_url(self.info_hash)));
        url_encoded.push_str(&format!("&peer_id={}", escape_bytes_url(self.peer_id)));
        url_encoded.push_str(&format!("&port={}", self.port));
        url_encoded.push_str(&format!("&uploaded={}", self.uploaded));
        url_encoded.push_str(&format!("&downloaded={}", self.downloaded));
        url_encoded.push_str(&format!("&left={}", self.left));
        url_encoded.push_str(&format!("&compact={}", self.compact));
        url_encoded
    }

    /// makes sequential requests to the urls passed in
    /// returns the response with the corresponding index
    ///
    /// note that the sequential comes from the BEP 12, I think its that way so we don't overload the trackers
    pub async fn get_first_response_in_list(
        &self,
        announce_urls: impl IntoIterator<Item = url::Url>,
    ) -> Option<(usize, TrackerResponse)> {
        let client = reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:142.0) Gecko/20100101 Firefox/142.0",
        )
        .build().ok()?;

        let query = self.to_url_encoded();
        let announce_urls = announce_urls
            .into_iter()
            .map(|mut url| {
                url.set_query(Some(&query));
                url
            })
            .enumerate();

        for (index, url) in announce_urls {
            if let Ok(response) = client.get(url).timeout(TRACKER_REQ_TIMEOUT).send().await
                && let Ok(bytes) = dbg!(response.bytes().await)
                && let Ok(tracker_response) = serde_bencode::from_bytes::<TrackerResponse>(&bytes)
            {
                return Some((index, tracker_response));
            }
        }

        None
    }
}

fn escape_bytes_url(bytes: &[u8; 20]) -> String {
    bytes
        .iter()
        .map(|b| {
            if b.is_ascii_alphanumeric() {
                (*b as char).to_string()
            } else {
                format!("%{}", hex::encode([*b]))
            }
        })
        .collect()
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrackerResponse {
    /// An integer, indicating how often your client should make a request to the tracker, in seconds.
    pub interval: usize,
    /// A string, which contains list of peers that your client can connect to.
    /// Each peer is represented using 6 bytes.
    /// The first 4 bytes are the peer's IP address and the last 2 bytes are the peer's port number.
    peers: PeerConnections,
    peers6: Option<Peer6Connections>,
}
impl TrackerResponse {
    pub fn get_peers(&self) -> Vec<SocketAddr> {
        let mut all_peers = self.peers.0.clone();
        all_peers.extend(self.peers6.clone().unwrap_or_default().0);
        all_peers.into_iter().collect()
    }
}

mod peers {
    use std::{
        fmt,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    use bytes::Bytes;
    use serde::{
        Deserialize, Deserializer,
        de::{self, Visitor},
    };

    struct PeersVisitor;

    #[derive(Debug, Clone)]
    pub(super) struct PeerConnections(pub(super) Vec<SocketAddr>);

    impl<'de> Visitor<'de> for PeersVisitor {
        type Value = PeerConnections;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("A string of multiples of 6 bytes")
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let length = v.len();
            if length % 6 == 0 {
                Ok(PeerConnections(
                    v.chunks_exact(6)
                        .map(|chunk| {
                            let &[a, b, c, d, p1, p2] =
                                chunk.try_into().expect("the chunk is of length 6");
                            SocketAddr::new(
                                Ipv4Addr::new(a, b, c, d).into(),
                                u16::from_be_bytes([p1, p2]),
                            )
                        })
                        .collect(),
                ))
            } else {
                Err(de::Error::custom(format!(
                    "Bytes which length is a multiple of 6. Got {:?}",
                    v.len()
                )))
            }
        }
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct PeerConnection {
        #[serde(rename = "peer id")]
        pub _peer_id: Bytes,
        ip: Bytes,
        port: u16,
    }

    impl<'de> Deserialize<'de> for PeerConnections {
        fn deserialize<D>(deserializer: D) -> Result<PeerConnections, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum Peers {
                Compact(Bytes),
                NonCompact(Vec<PeerConnection>),
            }

            let peers = Peers::deserialize(deserializer)?;
            match peers {
                Peers::Compact(bytes) => PeersVisitor.visit_bytes(&bytes),
                Peers::NonCompact(peer_connections) => Ok(peer_connections.into()),
            }
        }
    }

    impl From<Vec<PeerConnection>> for PeerConnections {
        fn from(peer_connections: Vec<PeerConnection>) -> Self {
            Self(
                peer_connections
                    .into_iter()
                    .filter_map(|peer| {
                        let ip_str = String::from_utf8(peer.ip.to_vec()).ok()?;
                        let ip_addr = ip_str.parse::<IpAddr>().ok()?;
                        Some((ip_addr, peer.port).into())
                    })
                    .collect(),
            )
        }
    }
}

mod peers6 {
    use std::{
        fmt,
        net::{IpAddr, Ipv6Addr, SocketAddr},
    };

    use serde::{
        Deserialize, Deserializer,
        de::{self, Visitor},
    };

    struct Peers6Visitor;

    #[derive(Debug, Clone, Default)]
    pub(super) struct Peer6Connections(pub(super) Vec<SocketAddr>);

    impl<'de> Visitor<'de> for Peers6Visitor {
        type Value = Peer6Connections;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("A string of multiples of 18 bytes")
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            let length = v.len();
            if length % 18 == 0 {
                Ok(Peer6Connections(
                    v.chunks_exact(18)
                        .map(|chunk| {
                            let ipv6_addr = Ipv6Addr::from_bits(u128::from_be_bytes(
                                chunk[..16].try_into().expect("chunk has 18 bytes"),
                            ));
                            let ip_addr = IpAddr::V6(ipv6_addr);
                            let port = u16::from_be_bytes(
                                chunk[16..18].try_into().expect("chunk has 18 bytes"),
                            );

                            SocketAddr::new(ip_addr, port)
                        })
                        .collect(),
                ))
            } else {
                Err(de::Error::custom(format!(
                    "Bytes which length is a multiple of 18. Got {:?}",
                    v.len()
                )))
            }
        }
    }

    impl<'de> Deserialize<'de> for Peer6Connections {
        fn deserialize<D>(deserializer: D) -> Result<Peer6Connections, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_bytes(Peers6Visitor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_bencode;
    #[test]
    fn test_deserialize_compact_peers() {
        // A dictionary with both 'peers' (IPv4) and 'peers6' (IPv6) keys
        let bencoded_response_with_ipv6 = b"d8:intervali0e5:peers6:\x7f\x00\x00\x01\x1a\x0e6:peers618:\x20\x01\x0d\xb8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x41\x37\x01\xbbee";
        let expected_socket_addrs_with_ipv6 = vec![
            "127.0.0.1:6670".parse::<SocketAddr>().unwrap(),
            "[2001:db8::4137]:443".parse::<SocketAddr>().unwrap(),
        ];
        let tracker_response_with_ipv6: TrackerResponse =
            serde_bencode::from_bytes(bencoded_response_with_ipv6).unwrap();
        assert_eq!(
            tracker_response_with_ipv6.get_peers(),
            expected_socket_addrs_with_ipv6
        );
    }

    #[test]
    fn test_deserialize_non_compact_peers() {
        // A dictionary with a 'peers' key containing a list of dictionaries
        let non_compact_peers =
        b"d8:intervali0e5:peersld2:ip9:127.0.0.17:peer id20:abcdefghij01234567894:porti6881eed2:ip13:2001:db8::4137:peer id20:abcdefghij01234567894:porti443eeee";
        let expected_socket_addrs = vec![
            "127.0.0.1:6881".parse::<SocketAddr>().unwrap(),
            "[2001:db8::413]:443".parse::<SocketAddr>().unwrap(),
        ];
        let tracker_response: TrackerResponse =
            serde_bencode::from_bytes(non_compact_peers).unwrap();
        assert_eq!(tracker_response.get_peers(), expected_socket_addrs);
    }
}
