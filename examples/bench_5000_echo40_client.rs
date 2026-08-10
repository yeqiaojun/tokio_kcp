use std::{
    env,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Semaphore,
    time::{Instant as TokioInstant, interval, interval_at, timeout},
};
use tokio_kcp::{KcpConfig, KcpNoDelayConfig, KcpStream};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let server_addr = arg(1, "127.0.0.1:3101").parse::<SocketAddr>().unwrap();
    let clients = arg(2, "5000").parse::<usize>().unwrap();
    let period = parse_period(&arg(3, "2"));
    let payload_len = arg(4, "40").parse::<usize>().unwrap();
    let duration_secs = arg(5, "30").parse::<u64>().unwrap();
    let concurrency = arg(6, &clients.to_string()).parse::<usize>().unwrap();

    assert_eq!(payload_len, 40, "this benchmark is fixed to 40-byte payloads");

    let connected = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let sent = Arc::new(AtomicU64::new(0));
    let echoed = Arc::new(AtomicU64::new(0));
    let latency_us = Arc::new(AtomicU64::new(0));
    let max_latency_us = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let run_for = Duration::from_secs(duration_secs);
    let semaphore = Arc::new(Semaphore::new(concurrency));

    for id in 0..clients {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let connected = connected.clone();
        let failed = failed.clone();
        let sent = sent.clone();
        let echoed = echoed.clone();
        let latency_us = latency_us.clone();
        let max_latency_us = max_latency_us.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let result = run_client(
                server_addr,
                id,
                period,
                run_for,
                sent,
                echoed,
                latency_us,
                max_latency_us,
            )
            .await;

            match result {
                Ok(()) => {
                    connected.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }

    let mut tick = interval(Duration::from_secs(5));
    loop {
        tick.tick().await;
        let elapsed = started.elapsed();
        let echoed_now = echoed.load(Ordering::Relaxed);
        let avg_latency_ms = if echoed_now == 0 {
            0.0
        } else {
            latency_us.load(Ordering::Relaxed) as f64 / echoed_now as f64 / 1000.0
        };

        println!("clients={clients}");
        println!("concurrency={concurrency}");
        println!("interval_ms={}", period.as_millis());
        println!("payload_len={payload_len}");
        println!("duration_secs={duration_secs}");
        println!("connected={}", connected.load(Ordering::Relaxed));
        println!("failed={}", failed.load(Ordering::Relaxed));
        println!("sent={}", sent.load(Ordering::Relaxed));
        println!("echoed={echoed_now}");
        println!("elapsed_ms={:.2}", elapsed.as_secs_f64() * 1000.0);
        println!("avg_latency_ms={avg_latency_ms:.2}");
        println!(
            "max_latency_ms={:.2}",
            max_latency_us.load(Ordering::Relaxed) as f64 / 1000.0
        );

        if elapsed >= Duration::from_secs(duration_secs + 30)
            || connected.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed) == clients
        {
            break;
        }
    }
}

fn arg(index: usize, default: &str) -> String {
    env::args().nth(index).unwrap_or_else(|| default.to_owned())
}

fn parse_period(value: &str) -> Duration {
    if let Some(ms) = value.strip_suffix("ms") {
        return Duration::from_millis(ms.parse::<u64>().unwrap());
    }

    Duration::from_secs(value.parse::<u64>().unwrap())
}

#[allow(clippy::too_many_arguments)]
async fn run_client(
    server_addr: SocketAddr,
    id: usize,
    period: Duration,
    run_for: Duration,
    sent: Arc<AtomicU64>,
    echoed: Arc<AtomicU64>,
    latency_us: Arc<AtomicU64>,
    max_latency_us: Arc<AtomicU64>,
) -> Result<(), ()> {
    let config = bench_config();
    let mut stream = timeout(Duration::from_secs(15), KcpStream::connect(&config, server_addr))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;
    let deadline = Instant::now() + run_for;

    let mut packet = [0u8; 40];
    packet[..8].copy_from_slice(&(id as u64).to_le_bytes());
    let mut response = [0u8; 40];
    let mut tick = interval_at(TokioInstant::now() + first_send_jitter(id, period), period);

    while Instant::now() < deadline {
        tick.tick().await;
        if Instant::now() >= deadline {
            break;
        }

        let started = Instant::now();
        stream.write_all(&packet).await.map_err(|_| ())?;
        sent.fetch_add(1, Ordering::Relaxed);

        stream.read_exact(&mut response).await.map_err(|_| ())?;
        if response != packet {
            return Err(());
        }

        let elapsed = started.elapsed().as_micros() as u64;
        latency_us.fetch_add(elapsed, Ordering::Relaxed);
        update_max(&max_latency_us, elapsed);
        echoed.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
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

fn first_send_jitter(id: usize, period: Duration) -> Duration {
    let period_ms = period.as_millis() as u64;
    if period_ms == 0 {
        return Duration::ZERO;
    }

    Duration::from_millis((id as u64).wrapping_mul(1_103_515_245).wrapping_add(12_345) % period_ms)
}

fn update_max(value: &AtomicU64, candidate: u64) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}
