use std::{
    io::{self, ErrorKind, Write},
    net::SocketAddr,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures_util::future;
use kcp::{Error as KcpError, Kcp, KcpResult};
use log::{error, trace};
use tokio::{net::UdpSocket, sync::mpsc};

use crate::{
    fec::{self, FecDecoder, FecEncoder},
    utils::now_millis_u64,
    KcpConfig,
};

const OUTPUT_BACKLOG: usize = 2048;

/// Writer for sending packets to the underlying UdpSocket
struct UdpOutput {
    post_tx: mpsc::Sender<Vec<u8>>,
}

impl UdpOutput {
    /// Create a new Writer for writing packets to UdpSocket
    pub fn new(socket: Arc<UdpSocket>, target_addr: SocketAddr, config: &KcpConfig) -> io::Result<UdpOutput> {
        let (post_tx, mut post_rx) = mpsc::channel::<Vec<u8>>(OUTPUT_BACKLOG);
        let mut fec_encoder = if config.fec.enabled() {
            Some(FecEncoder::new(config.fec.data_shards, config.fec.parity_shards)?)
        } else {
            None
        };

        tokio::spawn(async move {
            while let Some(buf) = post_rx.recv().await {
                if let Some(fec_encoder) = &mut fec_encoder {
                    match fec_encoder.encode(&buf) {
                        Ok(encoded) => {
                            for packet in encoded.packets {
                                send_packet(&socket, target_addr, &packet).await;
                            }
                        }
                        Err(err) => {
                            error!("[SEND] FEC encode failed, error: {}", err);
                        }
                    }
                } else {
                    send_packet(&socket, target_addr, &buf).await;
                }
            }
        });

        Ok(UdpOutput { post_tx })
    }
}

impl Write for UdpOutput {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.post_tx.try_send(buf.to_owned()) {
            Ok(()) => Ok(buf.len()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                trace!("[SEND] UDP output backlog full, dropped {} bytes", buf.len());
                Ok(buf.len())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(io::Error::new(ErrorKind::BrokenPipe, "UDP output task closed"))
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn send_packet(socket: &UdpSocket, target_addr: SocketAddr, buf: &[u8]) {
    match socket.try_send_to(buf, target_addr) {
        Ok(..) => {}
        Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
            if let Err(err) = socket.send_to(buf, target_addr).await {
                error!("[SEND] UDP send failed, error: {}", err);
            }
        }
        Err(err) => {
            error!("[SEND] UDP send failed, error: {}", err);
        }
    }
}

#[derive(Debug)]
pub struct KcpSocket {
    kcp: Kcp<UdpOutput>,
    last_update_ms: u64,
    update_interval_ms: u64,
    socket: Arc<UdpSocket>,
    flush_write: bool,
    flush_ack_input: bool,
    sent_first: bool,
    pending_sender: Option<Waker>,
    pending_receiver: Option<Waker>,
    closed: bool,
    allow_recv_empty_packet: bool,
    fec_decoder: Option<FecDecoder>,
}

impl KcpSocket {
    pub fn new(
        c: &KcpConfig,
        conv: u32,
        socket: Arc<UdpSocket>,
        target_addr: SocketAddr,
        stream: bool,
    ) -> KcpResult<KcpSocket> {
        c.validate()?;

        let output = UdpOutput::new(socket.clone(), target_addr, c)?;
        let mut kcp = if stream {
            Kcp::new_stream(conv, output)
        } else {
            Kcp::new(conv, output)
        };
        c.apply_config(&mut kcp);

        // Ask server to allocate one
        if conv == 0 {
            kcp.input_conv();
        }

        let now = now_millis_u64();
        kcp.update(now as u32)?;

        Ok(KcpSocket {
            kcp,
            last_update_ms: now,
            update_interval_ms: c.nodelay.interval.clamp(10, 5000) as u64,
            socket,
            flush_write: c.flush_write,
            flush_ack_input: c.flush_acks_input,
            sent_first: false,
            pending_sender: None,
            pending_receiver: None,
            closed: false,
            allow_recv_empty_packet: c.allow_recv_empty_packet,
            fec_decoder: if c.fec.enabled() {
                Some(FecDecoder::new(c.fec.data_shards, c.fec.parity_shards)?)
            } else {
                None
            },
        })
    }

    /// Call every time you got data from transmission
    pub fn input(&mut self, buf: &[u8]) -> KcpResult<bool> {
        if self.fec_decoder.is_some() && fec::is_fec_packet(buf) {
            return self.input_fec(buf);
        }

        self.input_kcp(buf)
    }

    fn input_fec(&mut self, buf: &[u8]) -> KcpResult<bool> {
        let mut waked = false;

        if let Some(payload) = fec::data_payload(buf) {
            waked |= self.input_kcp(payload)?;
        }

        let recovered = self
            .fec_decoder
            .as_mut()
            .unwrap()
            .decode(buf)
            .map_err(KcpError::IoError)?;

        for payload in recovered {
            waked |= self.input_kcp(&payload)?;
        }

        Ok(waked)
    }

    fn input_kcp(&mut self, buf: &[u8]) -> KcpResult<bool> {
        match self.kcp.input(buf) {
            Ok(..) => {}
            Err(KcpError::ConvInconsistent(expected, actual)) => {
                trace!("[INPUT] Conv expected={} actual={} ignored", expected, actual);
                return Ok(false);
            }
            Err(err) => return Err(err),
        }
        self.last_update_ms = now_millis_u64();

        if self.flush_ack_input {
            self.kcp.flush_ack()?;
        }

        Ok(self.try_wake_pending_waker())
    }

    /// Call if you want to send some data
    pub fn poll_send(&mut self, cx: &mut Context<'_>, mut buf: &[u8]) -> Poll<KcpResult<usize>> {
        if self.closed {
            return Err(io::Error::from(ErrorKind::BrokenPipe).into()).into();
        }

        // If:
        //     1. Have sent the first packet (asking for conv)
        //     2. Too many pending packets
        if self.sent_first
            && (self.kcp.wait_snd() >= self.kcp.snd_wnd() as usize
                || self.kcp.wait_snd() >= self.kcp.rmt_wnd() as usize
                || self.kcp.waiting_conv())
        {
            trace!(
                "[SEND] waitsnd={} sndwnd={} rmtwnd={} excceeded or waiting conv={}",
                self.kcp.wait_snd(),
                self.kcp.snd_wnd(),
                self.kcp.rmt_wnd(),
                self.kcp.waiting_conv()
            );

            if let Some(waker) = self.pending_sender.replace(cx.waker().clone()) {
                if !cx.waker().will_wake(&waker) {
                    waker.wake();
                }
            }
            return Poll::Pending;
        }

        if !self.sent_first && self.kcp.waiting_conv() && buf.len() > self.kcp.mss() {
            buf = &buf[..self.kcp.mss()];
        }

        let n = self.kcp.send(buf)?;
        self.sent_first = true;

        if self.kcp.wait_snd() >= self.kcp.snd_wnd() as usize || self.kcp.wait_snd() >= self.kcp.rmt_wnd() as usize {
            self.kcp.flush()?;
        }

        self.last_update_ms = now_millis_u64();

        if self.flush_write {
            self.kcp.flush()?;
        }

        Ok(n).into()
    }

    /// Call if you want to send some data
    #[allow(dead_code)]
    pub async fn send(&mut self, buf: &[u8]) -> KcpResult<usize> {
        future::poll_fn(|cx| self.poll_send(cx, buf)).await
    }

    #[allow(dead_code)]
    pub fn try_recv(&mut self, buf: &mut [u8]) -> KcpResult<usize> {
        if self.closed {
            return Ok(0);
        }
        self.kcp.recv(buf)
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<KcpResult<usize>> {
        if self.closed {
            return Ok(0).into();
        }

        match self.kcp.recv(buf) {
            e @ (Err(KcpError::RecvQueueEmpty) | Err(KcpError::ExpectingFragment)) => {
                trace!(
                    "[RECV] rcvwnd={} peeksize={} r={:?}",
                    self.kcp.rcv_wnd(),
                    self.kcp.peeksize().unwrap_or(0),
                    e
                );
            }
            Err(err) => return Err(err).into(),
            Ok(n) => {
                if n == 0 && !self.allow_recv_empty_packet {
                    trace!(
                        "[RECV] rcvwnd={} peeksize={} r=Ok(0)",
                        self.kcp.rcv_wnd(),
                        self.kcp.peeksize().unwrap_or(0),
                    );
                } else {
                    self.last_update_ms = now_millis_u64();
                    return Ok(n).into();
                }
            }
        }

        if let Some(waker) = self.pending_receiver.replace(cx.waker().clone()) {
            if !cx.waker().will_wake(&waker) {
                waker.wake();
            }
        }

        Poll::Pending
    }

    #[allow(dead_code)]
    pub async fn recv(&mut self, buf: &mut [u8]) -> KcpResult<usize> {
        future::poll_fn(|cx| self.poll_recv(cx, buf)).await
    }

    pub fn flush(&mut self) -> KcpResult<()> {
        self.kcp.flush()?;
        self.last_update_ms = now_millis_u64();
        Ok(())
    }

    fn try_wake_pending_sender(&mut self) -> bool {
        if self.pending_sender.is_some()
            && self.kcp.wait_snd() < self.kcp.snd_wnd() as usize
            && self.kcp.wait_snd() < self.kcp.rmt_wnd() as usize
            && !self.kcp.waiting_conv()
        {
            let waker = self.pending_sender.take().unwrap();
            waker.wake();

            return true;
        }

        false
    }

    fn try_wake_pending_receiver(&mut self) -> bool {
        if self.pending_receiver.is_some() {
            if let Ok(peek) = self.kcp.peeksize() {
                if self.allow_recv_empty_packet || peek > 0 {
                    let waker = self.pending_receiver.take().unwrap();
                    waker.wake();

                    return true;
                }
            }
        }

        false
    }

    fn try_wake_pending_waker(&mut self) -> bool {
        self.try_wake_pending_sender() | self.try_wake_pending_receiver()
    }

    pub fn update_at(&mut self, now_ms: u64) -> KcpResult<Duration> {
        let now = now_ms as u32;
        self.kcp.update(now)?;
        let next = if self.kcp.wait_snd() == 0 {
            self.update_interval_ms
        } else {
            self.kcp.check(now) as u64
        };

        self.try_wake_pending_sender();

        Ok(Duration::from_millis(next))
    }

    pub fn update(&mut self) -> KcpResult<Duration> {
        self.update_at(now_millis_u64())
    }

    pub fn close(&mut self) {
        self.closed = true;
        if let Some(w) = self.pending_sender.take() {
            w.wake();
        }
        if let Some(w) = self.pending_receiver.take() {
            w.wake();
        }
    }

    pub fn udp_socket(&self) -> &Arc<UdpSocket> {
        &self.socket
    }

    pub fn can_close(&self) -> bool {
        self.kcp.wait_snd() == 0
    }

    pub fn conv(&self) -> u32 {
        self.kcp.conv()
    }

    pub fn set_conv(&mut self, conv: u32) {
        self.kcp.set_conv(conv);
    }

    pub fn waiting_conv(&self) -> bool {
        self.kcp.waiting_conv()
    }

    pub fn peek_size(&self) -> KcpResult<usize> {
        self.kcp.peeksize()
    }

    pub fn last_update_ms(&self) -> u64 {
        self.last_update_ms
    }
}

#[cfg(test)]
mod test {

    use kcp::Error as KcpError;
    use log::trace;
    use std::sync::Arc;
    use tokio::{net::UdpSocket, sync::Mutex, time};

    use super::KcpSocket;
    use crate::config::KcpConfig;

    #[tokio::test]
    async fn kcp_echo() {
        let _ = env_logger::try_init();

        static CONV: u32 = 0xdeadbeef;

        // s1 connects s2
        let s1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let s2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let s1_addr = s1.local_addr().unwrap();
        let s2_addr = s2.local_addr().unwrap();

        let s1 = Arc::new(s1);
        let s2 = Arc::new(s2);

        let config = KcpConfig::default();
        let kcp1 = KcpSocket::new(&config, 0, s1.clone(), s2_addr, true).unwrap();
        let kcp2 = KcpSocket::new(&config, CONV, s2.clone(), s1_addr, true).unwrap();

        let kcp1 = Arc::new(Mutex::new(kcp1));
        let kcp2 = Arc::new(Mutex::new(kcp2));

        let kcp1_task = {
            let kcp1 = kcp1.clone();
            tokio::spawn(async move {
                loop {
                    let mut kcp = kcp1.lock().await;
                    let next = kcp.update().expect("update");
                    trace!("kcp1 next tick {:?}", next);
                    time::sleep(next).await;
                }
            })
        };

        let kcp2_task = {
            let kcp2 = kcp2.clone();
            tokio::spawn(async move {
                loop {
                    let mut kcp = kcp2.lock().await;
                    let next = kcp.update().expect("update");
                    trace!("kcp2 next tick {:?}", next);
                    time::sleep(next).await;
                }
            })
        };

        const SEND_BUFFER: &[u8] = b"HELLO WORLD";

        {
            let n = kcp1.lock().await.send(SEND_BUFFER).await.unwrap();
            assert_eq!(n, SEND_BUFFER.len());
        }

        let echo_task = tokio::spawn(async move {
            let mut buf = [0u8; 1024];

            loop {
                let n = s2.recv(&mut buf).await.unwrap();

                let packet = &mut buf[..n];

                let conv = kcp::get_conv(packet);
                if conv == 0 {
                    kcp::set_conv(packet, CONV);
                }

                let mut kcp2 = kcp2.lock().await;
                kcp2.input(packet).unwrap();

                match kcp2.try_recv(&mut buf) {
                    Ok(n) => {
                        let received = &buf[..n];
                        kcp2.send(received).await.unwrap();
                    }
                    Err(KcpError::RecvQueueEmpty) => {
                        continue;
                    }
                    Err(err) => {
                        panic!("kcp.recv error: {:?}", err);
                    }
                }
            }
        });

        {
            let mut buf = [0u8; 1024];

            loop {
                let n = s1.recv(&mut buf).await.unwrap();

                let packet = &buf[..n];

                let mut kcp1 = kcp1.lock().await;
                kcp1.input(packet).unwrap();

                match kcp1.try_recv(&mut buf) {
                    Ok(n) => {
                        let received = &buf[..n];
                        assert_eq!(received, SEND_BUFFER);
                        break;
                    }
                    Err(KcpError::RecvQueueEmpty) => {
                        continue;
                    }
                    Err(err) => {
                        panic!("kcp.recv error: {:?}", err);
                    }
                }
            }
        }

        echo_task.abort();
        kcp1_task.abort();
        kcp2_task.abort();
    }
}
