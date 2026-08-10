use std::{
    env,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time,
};
use tokio_kcp::{KcpConfig, KcpListener, KcpNoDelayConfig};

const PAYLOAD_LEN: usize = 40;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:3101".to_owned())
        .parse::<SocketAddr>()
        .unwrap();
    let run_for = env::args()
        .nth(2)
        .map(|s| Duration::from_secs(s.parse::<u64>().unwrap()));

    let mut listener = KcpListener::bind(bench_config(), addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let accepted = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let rx_packets = Arc::new(AtomicU64::new(0));
    let tx_packets = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    println!("server_addr={local_addr}");
    println!("payload_len={PAYLOAD_LEN}");
    if let Some(run_for) = run_for {
        println!("run_for_secs={}", run_for.as_secs());
    }

    {
        let accepted = accepted.clone();
        let active = active.clone();
        let rx_packets = rx_packets.clone();
        let tx_packets = tx_packets.clone();
        let errors = errors.clone();
        tokio::spawn(async move {
            let mut tick = time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                println!(
                    "accepted={} active={} rx_packets={} tx_packets={} errors={}",
                    accepted.load(Ordering::Relaxed),
                    active.load(Ordering::Relaxed),
                    rx_packets.load(Ordering::Relaxed),
                    tx_packets.load(Ordering::Relaxed),
                    errors.load(Ordering::Relaxed)
                );
            }
        });
    }

    let mut stop = run_for.map(|duration| Box::pin(time::sleep(duration)));

    loop {
        let accepted_stream = if let Some(stop) = stop.as_mut() {
            tokio::select! {
                _ = stop.as_mut() => break,
                accepted = listener.accept() => accepted.unwrap(),
            }
        } else {
            listener.accept().await.unwrap()
        };
        let (mut stream, _) = accepted_stream;
        accepted.fetch_add(1, Ordering::Relaxed);
        active.fetch_add(1, Ordering::Relaxed);

        let active = active.clone();
        let rx_packets = rx_packets.clone();
        let tx_packets = tx_packets.clone();
        let errors = errors.clone();

        tokio::spawn(async move {
            let mut packet = [0u8; PAYLOAD_LEN];
            loop {
                if stream.read_exact(&mut packet).await.is_err() {
                    break;
                }
                rx_packets.fetch_add(1, Ordering::Relaxed);

                if stream.write_all(&packet).await.is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                tx_packets.fetch_add(1, Ordering::Relaxed);
            }
            active.fetch_sub(1, Ordering::Relaxed);
        });
    }

    println!(
        "server_done accepted={} active={} rx_packets={} tx_packets={} errors={}",
        accepted.load(Ordering::Relaxed),
        active.load(Ordering::Relaxed),
        rx_packets.load(Ordering::Relaxed),
        tx_packets.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed)
    );
}

fn bench_config() -> KcpConfig {
    KcpConfig {
        nodelay: KcpNoDelayConfig {
            nodelay: true,
            interval: 50,
            resend: 2,
            nc: false,
        },
        wnd_size: (128, 128),
        flush_write: true,
        flush_acks_input: false,
        ..KcpConfig::default()
    }
}
