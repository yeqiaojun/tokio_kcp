use std::{
    io::{self, ErrorKind},
    net::SocketAddr,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use byte_string::ByteStr;
use kcp::{Error as KcpError, KcpResult};
use log::{debug, error, trace};
use tokio::{
    net::{ToSocketAddrs, UdpSocket},
    sync::mpsc,
    task::JoinHandle,
    time,
};

use crate::{config::KcpConfig, fec, session::KcpSessionManager, stream::KcpStream};

#[derive(Debug)]
pub struct KcpListener {
    udp: Arc<UdpSocket>,
    accept_rx: mpsc::Receiver<(KcpStream, SocketAddr)>,
    task_watcher: JoinHandle<()>,
}

impl Drop for KcpListener {
    fn drop(&mut self) {
        self.task_watcher.abort();
    }
}

impl KcpListener {
    /// Create an `KcpListener` with kcp-go compatible FEC options.
    pub async fn listen_with_options<A: ToSocketAddrs>(
        addr: A,
        data_shards: usize,
        parity_shards: usize,
    ) -> KcpResult<KcpListener> {
        let config = KcpConfig::default().with_fec(data_shards, parity_shards);
        KcpListener::bind(config, addr).await
    }

    /// Create an `KcpListener` bound to `addr`
    pub async fn bind<A: ToSocketAddrs>(config: KcpConfig, addr: A) -> KcpResult<KcpListener> {
        let udp = UdpSocket::bind(addr).await?;
        KcpListener::from_socket(config, udp).await
    }

    /// Create a `KcpListener` from an existed `UdpSocket`
    pub async fn from_socket(config: KcpConfig, udp: UdpSocket) -> KcpResult<KcpListener> {
        config.validate()?;

        let udp = Arc::new(udp);
        let server_udp = udp.clone();

        let (accept_tx, accept_rx) = mpsc::channel(1024 /* backlogs */);
        let task_watcher = tokio::spawn(async move {
            let (close_tx, mut close_rx) = mpsc::unbounded_channel();

            let mut sessions = KcpSessionManager::new();
            let mut packet_buffer = [0u8; 65536];
            loop {
                tokio::select! {
                    peer_addr = close_rx.recv() => {
                        let peer_addr = peer_addr.expect("close_tx closed unexpectedly");
                        sessions.close_peer(peer_addr);
                        trace!("session peer_addr: {} removed", peer_addr);
                    }

                    recv_res = udp.recv_from(&mut packet_buffer) => {
                        match recv_res {
                            Err(err) => {
                                error!("udp.recv_from failed, error: {}", err);
                                time::sleep(Duration::from_secs(1)).await;
                            }
                            Ok((n, peer_addr)) => {
                                let packet = &mut packet_buffer[..n];

                                trace!("received peer: {}, {:?}", peer_addr, ByteStr::new(packet));

                                if config.fec.enabled() && fec::is_parity_packet(packet) {
                                    if let Some(session) = sessions.get(peer_addr) {
                                        if session.input(packet).await.is_err() {
                                            trace!("[SESSION] KCP session is closing while listener tries to input");
                                        }
                                    }
                                    continue;
                                }

                                let (conv, sn) = {
                                    let is_fec_data = config.fec.enabled() && fec::is_data_packet(packet);
                                    let kcp_packet = if is_fec_data {
                                        fec::data_payload_mut(packet).unwrap()
                                    } else {
                                        &mut *packet
                                    };

                                    if kcp_packet.len() < kcp::KCP_OVERHEAD {
                                        error!("packet too short, received {} bytes, but at least {} bytes",
                                               kcp_packet.len(),
                                               kcp::KCP_OVERHEAD);
                                        continue;
                                    }

                                    let mut conv = kcp::get_conv(kcp_packet);
                                    if conv == 0 {
                                        // Allocate a conv for client.
                                        conv = sessions.alloc_conv();
                                        debug!("allocate {} conv for peer: {}", conv, peer_addr);

                                        kcp::set_conv(kcp_packet, conv);
                                    }

                                    (conv, kcp::get_sn(kcp_packet))
                                };

                                let session = match sessions.get_or_create(&config, conv, sn, &udp, peer_addr, &close_tx).await {
                                    Ok((s, created)) => {
                                        if created {
                                            // Created a new session, constructed a new accepted client
                                            let stream = KcpStream::with_session(s.clone());
                                            if  accept_tx.try_send((stream, peer_addr)).is_err() {
                                                debug!("failed to create accepted stream due to channel failure");

                                                // remove it from session
                                                sessions.close_peer(peer_addr);
                                                continue;
                                            }
                                        } else {
                                            let session_conv = s.conv().await;
                                            if session_conv != conv {
                                                debug!("received peer: {} with conv: {} not match with session conv: {}",
                                                       peer_addr,
                                                       conv,
                                                       session_conv);
                                                continue;
                                            }
                                        }

                                        s
                                    },
                                    Err(err) => {
                                        error!("failed to create session, error: {}, peer: {}, conv: {}", err, peer_addr, conv);
                                        continue;
                                    }
                                };

                                // let mut kcp = session.kcp_socket().lock().await;
                                // if let Err(err) = kcp.input(packet) {
                                //     error!("kcp.input failed, peer: {}, conv: {}, error: {}, packet: {:?}", peer_addr, conv, err, ByteStr::new(packet));
                                // }
                                if session.input(packet).await.is_err() {
                                    trace!("[SESSION] KCP session is closing while listener tries to input");
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(KcpListener {
            udp: server_udp,
            accept_rx,
            task_watcher,
        })
    }

    /// Accept a new connected `KcpStream`
    pub async fn accept(&mut self) -> KcpResult<(KcpStream, SocketAddr)> {
        match self.accept_rx.recv().await {
            Some(s) => Ok(s),
            None => Err(KcpError::IoError(io::Error::new(
                ErrorKind::Other,
                "accept channel closed unexpectedly",
            ))),
        }
    }

    pub fn poll_accept(&mut self, cx: &mut Context<'_>) -> Poll<KcpResult<(KcpStream, SocketAddr)>> {
        self.accept_rx.poll_recv(cx).map(|op_res| {
            op_res.ok_or_else(|| {
                KcpError::IoError(io::Error::new(ErrorKind::Other, "accept channel closed unexpectedly"))
            })
        })
    }

    /// Get the local address of the underlying socket
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.udp.local_addr()
    }
}

#[cfg(unix)]
impl std::os::unix::io::AsRawFd for KcpListener {
    fn as_raw_fd(&self) -> std::os::unix::prelude::RawFd {
        self.udp.as_raw_fd()
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsRawSocket for KcpListener {
    fn as_raw_socket(&self) -> std::os::windows::prelude::RawSocket {
        self.udp.as_raw_socket()
    }
}

#[cfg(test)]
mod test {
    use futures_util::future;
    use kcp::Error as KcpError;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::KcpListener;
    use crate::{config::KcpConfig, fec::FEC_HEADER_SIZE_PLUS_SIZE, stream::KcpStream};

    #[tokio::test]
    async fn multi_echo() {
        let _ = env_logger::try_init();

        let config = KcpConfig::default();

        let mut listener = KcpListener::bind(config, "127.0.0.1:0").await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();

                tokio::spawn(async move {
                    let mut buffer = [0u8; 8192];
                    while let Ok(n) = stream.read(&mut buffer).await {
                        if n == 0 {
                            break;
                        }

                        let data = &buffer[..n];
                        stream.write_all(data).await.unwrap();
                        stream.flush().await.unwrap();
                    }
                });
            }
        });

        let mut vfut = Vec::new();

        for _ in 0..100 {
            vfut.push(async move {
                let mut stream = KcpStream::connect(&config, server_addr).await.unwrap();

                for _ in 0..20 {
                    const SEND_BUFFER: &[u8] = b"HELLO WORLD";
                    stream.write_all(SEND_BUFFER).await.unwrap();
                    stream.flush().await.unwrap();

                    let mut buffer = [0u8; 1024];
                    let n = stream.recv(&mut buffer).await.unwrap();
                    assert_eq!(SEND_BUFFER, &buffer[..n]);
                }
            });
        }

        future::join_all(vfut).await;
    }

    #[tokio::test]
    async fn fec_echo() {
        let _ = env_logger::try_init();

        let mut listener = KcpListener::listen_with_options("127.0.0.1:0", 2, 1).await.unwrap();
        let server_addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 8192];
            let n = stream.read(&mut buffer).await.unwrap();
            stream.write_all(&buffer[..n]).await.unwrap();
            stream.flush().await.unwrap();
        });

        let mut stream = KcpStream::dial_with_options(server_addr, 2, 1).await.unwrap();
        stream.write_all(b"HELLO FEC").await.unwrap();
        stream.flush().await.unwrap();

        let mut buffer = [0u8; 1024];
        let n = stream.read(&mut buffer).await.unwrap();
        assert_eq!(&buffer[..n], b"HELLO FEC");
    }

    #[tokio::test]
    async fn fec_rejects_mtu_too_small_for_header() {
        let mut config = KcpConfig::default().with_fec(2, 1);
        config.mtu = FEC_HEADER_SIZE_PLUS_SIZE;

        match KcpListener::bind(config, "127.0.0.1:0").await {
            Err(KcpError::IoError(err)) => assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected invalid input, got {:?}", other),
        }
    }
}
