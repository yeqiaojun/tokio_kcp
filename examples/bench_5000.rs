use std::{
    env,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, Semaphore},
    time::timeout,
};
use tokio_kcp::{KcpConfig, KcpListener, KcpStream};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let clients = env::args()
        .nth(1)
        .and_then(|arg| arg.parse::<usize>().ok())
        .unwrap_or(5000);
    let concurrency = env::args()
        .nth(2)
        .and_then(|arg| arg.parse::<usize>().ok())
        .unwrap_or(clients);
    let hold = env::args()
        .nth(3)
        .and_then(|arg| arg.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::ZERO);

    let config = KcpConfig::default();
    let mut listener = KcpListener::bind(config, "127.0.0.1:0").await.unwrap();
    let server_addr = listener.local_addr().unwrap();

    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_server = accepted.clone();
    tokio::spawn(async move {
        while accepted_server.load(Ordering::Relaxed) < clients {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            accepted_server.fetch_add(1, Ordering::Relaxed);

            tokio::spawn(async move {
                let mut buffer = [0u8; 64];
                let Ok(Ok(n)) = timeout(Duration::from_secs(15), stream.read(&mut buffer)).await else {
                    return;
                };
                if n == 0 {
                    return;
                }
                let _ = timeout(Duration::from_secs(15), async {
                    stream.write_all(&buffer[..n]).await?;
                    stream.flush().await
                })
                .await;
                tokio::time::sleep(hold).await;
            });
        }
    });

    let started = Instant::now();
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let (result_tx, mut result_rx) = mpsc::channel::<Result<Duration, String>>(clients);

    for idx in 0..clients {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let tx = result_tx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = run_client(server_addr, idx, hold).await;
            let _ = tx.send(result).await;
        });
    }
    drop(result_tx);

    let mut ok = 0usize;
    let mut failed = 0usize;
    let mut latency_sum = Duration::ZERO;
    let mut latency_max = Duration::ZERO;
    let mut first_errors = Vec::new();

    while let Some(result) = result_rx.recv().await {
        match result {
            Ok(latency) => {
                ok += 1;
                latency_sum += latency;
                latency_max = latency_max.max(latency);
            }
            Err(err) => {
                failed += 1;
                if first_errors.len() < 10 {
                    first_errors.push(err);
                }
            }
        }
    }

    let elapsed = started.elapsed();
    let avg_latency_ms = if ok == 0 {
        0.0
    } else {
        latency_sum.as_secs_f64() * 1000.0 / ok as f64
    };

    println!("clients={clients}");
    println!("concurrency={concurrency}");
    println!("hold_secs={:.2}", hold.as_secs_f64());
    println!("accepted={}", accepted.load(Ordering::Relaxed));
    println!("ok={ok}");
    println!("failed={failed}");
    println!("elapsed_ms={:.2}", elapsed.as_secs_f64() * 1000.0);
    println!("avg_latency_ms={avg_latency_ms:.2}");
    println!("max_latency_ms={:.2}", latency_max.as_secs_f64() * 1000.0);
    if !first_errors.is_empty() {
        println!("first_errors={first_errors:?}");
    }
}

async fn run_client(server_addr: SocketAddr, idx: usize, hold: Duration) -> Result<Duration, String> {
    let config = KcpConfig::default();
    let payload = format!("bench-{idx}");
    let started = Instant::now();

    let mut stream = timeout(Duration::from_secs(15), KcpStream::connect(&config, server_addr))
        .await
        .map_err(|_| "connect timeout".to_owned())?
        .map_err(|err| format!("connect: {err}"))?;

    timeout(Duration::from_secs(15), async {
        stream
            .write_all(payload.as_bytes())
            .await
            .map_err(|err| format!("write: {err}"))?;
        stream.flush().await.map_err(|err| format!("flush: {err}"))?;

        let mut buffer = [0u8; 64];
        let n = stream.read(&mut buffer).await.map_err(|err| format!("read: {err}"))?;
        if &buffer[..n] != payload.as_bytes() {
            return Err("echo mismatch".to_owned());
        }
        Ok(())
    })
    .await
    .map_err(|_| "io timeout".to_owned())??;

    let echo_latency = started.elapsed();
    tokio::time::sleep(hold).await;

    Ok(echo_latency)
}
