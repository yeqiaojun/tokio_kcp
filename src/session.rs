use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::{self, Debug},
    io::ErrorKind,
    net::SocketAddr,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use byte_string::ByteStr;
use kcp::{Error as KcpError, KcpResult};
use log::{error, trace};
use spin::Mutex as SpinMutex;
use spin::MutexGuard as SpinMutexGuard;
use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle};

use crate::{fec, scheduler::session_scheduler, skcp::KcpSocket, KcpConfig};

pub struct KcpSession {
    socket: SpinMutex<KcpSocket>,
    closed: AtomicBool,
    session_expire_ms: u64,
    session_close_notifier: Option<(mpsc::UnboundedSender<SocketAddr>, SocketAddr)>,
    input_tx: mpsc::Sender<Vec<u8>>,
    io_task_handle: SpinMutex<Option<JoinHandle<()>>>,
}

impl Drop for KcpSession {
    fn drop(&mut self) {
        trace!(
            "[SESSION] KcpSession conv {} is dropping, closed? {}",
            self.lock_socket().conv(),
            self.closed.load(Ordering::Acquire),
        );
    }
}

impl Debug for KcpSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let socket = self.lock_socket();
        f.debug_struct("KcpSession")
            .field("socket", socket.deref())
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .field("session_expire_ms", &self.session_expire_ms)
            .field("session_close_notifier", &self.session_close_notifier)
            .field("input_tx", &self.input_tx)
            .finish()
    }
}

impl KcpSession {
    fn new(
        socket: KcpSocket,
        session_expire: Option<Duration>,
        session_close_notifier: Option<(mpsc::UnboundedSender<SocketAddr>, SocketAddr)>,
        input_tx: mpsc::Sender<Vec<u8>>,
    ) -> KcpSession {
        let session_expire_ms = if session_close_notifier.is_some() {
            session_expire.map_or(0, |expire| expire.as_millis() as u64)
        } else {
            0
        };

        KcpSession {
            socket: SpinMutex::new(socket),
            closed: AtomicBool::new(false),
            session_expire_ms,
            session_close_notifier,
            input_tx,
            io_task_handle: SpinMutex::new(None),
        }
    }

    pub fn new_shared(
        socket: KcpSocket,
        session_expire: Option<Duration>,
        session_close_notifier: Option<(mpsc::UnboundedSender<SocketAddr>, SocketAddr)>,
    ) -> Arc<KcpSession> {
        let is_client = session_close_notifier.is_none();
        let (input_tx, mut input_rx) = mpsc::channel(64);

        let udp_socket = socket.udp_socket().clone();

        let session = Arc::new(KcpSession::new(
            socket,
            session_expire,
            session_close_notifier,
            input_tx,
        ));

        let io_task_handle = {
            let session = session.clone();
            tokio::spawn(async move {
                let mut input_buffer = [0u8; 65536];

                loop {
                    tokio::select! {
                        // recv() then input()
                        // Drives the KCP machine forward
                        recv_result = udp_socket.recv(&mut input_buffer), if is_client => {
                            match recv_result {
                                Err(err) => {
                                    error!("[SESSION] UDP recv failed, error: {}", err);
                                    session.closed.store(true, Ordering::Release);
                                    break;
                                }
                                Ok(n) => {
                                    let input_buffer = &input_buffer[..n];

                                    let input_conv = if let Some(payload) = fec::data_payload(input_buffer) {
                                        if payload.len() < kcp::KCP_OVERHEAD {
                                            error!("packet too short, received {} bytes, but at least {} bytes",
                                                   payload.len(),
                                                   kcp::KCP_OVERHEAD);
                                            continue;
                                        }

                                        Some(kcp::get_conv(payload))
                                    } else if fec::is_parity_packet(input_buffer) {
                                        None
                                    } else {
                                        if input_buffer.len() < kcp::KCP_OVERHEAD {
                                            error!("packet too short, received {} bytes, but at least {} bytes",
                                                   input_buffer.len(),
                                                   kcp::KCP_OVERHEAD);
                                            continue;
                                        }

                                        Some(kcp::get_conv(input_buffer))
                                    };

                                    if input_conv.is_none() {
                                        let mut socket = session.lock_socket();
                                        match socket.input(input_buffer) {
                                            Ok(true) => {
                                                trace!("[SESSION] UDP input {} bytes and waked sender/receiver", n);
                                            }
                                            Ok(false) => {}
                                            Err(err) => {
                                                error!("[SESSION] UDP input {} bytes error: {}, input buffer {:?}",
                                                       n, err, ByteStr::new(input_buffer));
                                            }
                                        }
                                        continue;
                                    }

                                    let input_conv = input_conv.unwrap();
                                    trace!("[SESSION] UDP recv {} bytes, conv: {}, going to input {:?}",
                                           n, input_conv, ByteStr::new(input_buffer));

                                    let mut socket = session.lock_socket();

                                    // Server may allocate another conv for this client.
                                    if !socket.waiting_conv() && socket.conv() != input_conv {
                                        trace!("[SESSION] UDP input conv: {} replaces session conv: {}", input_conv, socket.conv());
                                        socket.set_conv(input_conv);
                                    }

                                    match socket.input(input_buffer) {
                                        Ok(true) => {
                                            trace!("[SESSION] UDP input {} bytes and waked sender/receiver", n);
                                        }
                                        Ok(false) => {}
                                        Err(err) => {
                                            error!("[SESSION] UDP input {} bytes error: {}, input buffer {:?}",
                                                   n, err, ByteStr::new(input_buffer));
                                        }
                                    }
                                }
                            }
                        }

                        // bytes received from listener socket
                        input_opt = input_rx.recv() => {
                            if let Some(input_buffer) = input_opt {
                                let mut socket = session.lock_socket();
                                match socket.input(&input_buffer) {
                                    Ok(waked) => {
                                        // trace!("[SESSION] UDP input {} bytes from channel {:?}",
                                        //        input_buffer.len(), ByteStr::new(&input_buffer));
                                        trace!("[SESSION] UDP input {} bytes from channel, waked? {} sender/receiver",
                                               input_buffer.len(), waked);
                                    }
                                    Err(err) => {
                                        error!("[SESSION] UDP input {} bytes from channel failed, error: {}, input buffer {:?}",
                                               input_buffer.len(), err, ByteStr::new(&input_buffer));
                                    }
                                }
                            }
                        }
                    }
                }
            })
        };
        *session.io_task_handle.lock() = Some(io_task_handle);
        session_scheduler().register(session.clone());

        session
    }

    pub fn kcp_socket(&self) -> &SpinMutex<KcpSocket> {
        &self.socket
    }

    pub(crate) fn lock_socket(&self) -> SpinMutexGuard<'_, KcpSocket> {
        self.socket.lock()
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify();
    }

    pub async fn input(&self, buf: &[u8]) -> Result<(), SessionClosedError> {
        self.input_tx.send(buf.to_owned()).await.map_err(|_| SessionClosedError)
    }

    pub async fn conv(&self) -> u32 {
        let socket = self.lock_socket();
        socket.conv()
    }

    #[inline]
    pub fn notify(&self) {}

    pub(crate) fn poll_scheduler_tick(&self, now_ms: u64) -> Option<u64> {
        match self.drive_update(now_ms) {
            Some(next_update_ms) => Some(next_update_ms),
            None => {
                self.finalize();
                None
            }
        }
    }

    fn drive_update(&self, now_ms: u64) -> Option<u64> {
        let mut socket = self.lock_socket();

        let is_closed = self.closed.load(Ordering::Acquire);
        if is_closed && socket.can_close() {
            trace!("[SESSION] KCP session closing");
            return None;
        }

        let expire_ms = self.session_expire_ms;
        if expire_ms != 0 {
            let elapsed_ms = now_ms.saturating_sub(socket.last_update_ms());

            if elapsed_ms > expire_ms {
                if elapsed_ms > expire_ms.saturating_mul(2) {
                    trace!(
                        "[SESSION] force close inactive session, conv: {}, last_update: {}s ago",
                        socket.conv(),
                        elapsed_ms / 1000
                    );
                    return None;
                }

                if !is_closed {
                    trace!(
                        "[SESSION] closing inactive session, conv: {}, last_update: {}s ago",
                        socket.conv(),
                        elapsed_ms / 1000
                    );
                    self.closed.store(true, Ordering::Release);
                }
            }
        }

        let next_delay = match socket.update_at(now_ms) {
            Ok(next_delay) => next_delay,
            Err(KcpError::IoError(err)) if err.kind() == ErrorKind::BrokenPipe => {
                trace!("[SESSION] KCP output closed");
                return None;
            }
            Err(err) => {
                error!("[SESSION] KCP update failed, error: {}", err);
                Duration::from_millis(10)
            }
        };

        Some(now_ms.saturating_add(next_delay.as_millis() as u64))
    }

    fn finalize(&self) {
        {
            let mut socket = self.lock_socket();
            socket.close();
        }

        if let Some(io_task_handle) = self.io_task_handle.lock().take() {
            io_task_handle.abort();
        }

        if let Some((notifier, peer_addr)) = &self.session_close_notifier {
            let _ = notifier.send(*peer_addr);
        }

        self.closed.store(true, Ordering::Release);
        trace!("[SESSION] KCP session closed");
    }
}

pub struct SessionClosedError;

struct KcpSessionUniq(Arc<KcpSession>);

impl Drop for KcpSessionUniq {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl Deref for KcpSessionUniq {
    type Target = KcpSession;

    fn deref(&self) -> &KcpSession {
        &self.0
    }
}

pub struct KcpSessionManager {
    sessions: HashMap<SocketAddr, KcpSessionUniq>,
}

impl KcpSessionManager {
    pub fn new() -> KcpSessionManager {
        KcpSessionManager {
            sessions: HashMap::new(),
        }
    }

    #[inline]
    pub fn alloc_conv(&mut self) -> u32 {
        let mut conv = rand::random();
        while conv == 0 {
            conv = rand::random()
        }
        conv
    }

    pub fn close_peer(&mut self, peer_addr: SocketAddr) {
        self.sessions.remove(&peer_addr);
    }

    pub fn get(&self, peer_addr: SocketAddr) -> Option<Arc<KcpSession>> {
        self.sessions.get(&peer_addr).map(|session| session.0.clone())
    }

    pub async fn get_or_create(
        &mut self,
        config: &KcpConfig,
        conv: u32,
        sn: u32,
        udp: &Arc<UdpSocket>,
        peer_addr: SocketAddr,
        session_close_notifier: &mpsc::UnboundedSender<SocketAddr>,
    ) -> KcpResult<(Arc<KcpSession>, bool)> {
        match self.sessions.entry(peer_addr) {
            Entry::Occupied(mut occ) => {
                let session = occ.get();

                if sn == 0 && session.conv().await != conv {
                    // This is the first packet received from this peer.
                    // Recreate a new session for this specific client.

                    let socket = KcpSocket::new(config, conv, udp.clone(), peer_addr, config.stream)?;
                    let session = KcpSession::new_shared(
                        socket,
                        config.session_expire,
                        Some((session_close_notifier.clone(), peer_addr)),
                    );

                    let old_session = occ.insert(KcpSessionUniq(session.clone()));
                    let old_conv = old_session.conv().await;
                    trace!(
                        "replaced session with conv: {} (old: {}), peer: {}",
                        conv,
                        old_conv,
                        peer_addr
                    );

                    Ok((session, true))
                } else {
                    Ok((session.0.clone(), false))
                }
            }
            Entry::Vacant(vac) => {
                let socket = KcpSocket::new(config, conv, udp.clone(), peer_addr, config.stream)?;
                let session = KcpSession::new_shared(
                    socket,
                    config.session_expire,
                    Some((session_close_notifier.clone(), peer_addr)),
                );
                trace!("created session for conv: {}, peer: {}", conv, peer_addr);
                vac.insert(KcpSessionUniq(session.clone()));
                Ok((session, true))
            }
        }
    }
}
