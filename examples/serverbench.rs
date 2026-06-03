use std::{
    env,
    error::Error,
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{watch, Semaphore},
    time,
};
use tokio_kcp::{KcpConfig, KcpListener, KcpNoDelayConfig, KcpStream};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug)]
struct Options {
    mode: String,
    addr: SocketAddr,
    sessions: usize,
    payload_size: usize,
    interval: Duration,
    duration: Duration,
    timeout: Duration,
    dial_parallel: usize,
    ready_file: PathBuf,
    start_file: PathBuf,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            mode: "server".to_owned(),
            addr: "127.0.0.1:40001".parse().unwrap(),
            sessions: 5000,
            payload_size: 40,
            interval: Duration::from_millis(200),
            duration: Duration::from_secs(30),
            timeout: Duration::from_secs(2),
            dial_parallel: 64,
            ready_file: ".codex_tmp/serverbench.ready".into(),
            start_file: ".codex_tmp/serverbench.start".into(),
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(err) = async_main().await {
        eprintln!("serverbench failed: {err}");
        std::process::exit(1);
    }
}

async fn async_main() -> BenchResult<()> {
    let opts = parse_args()?;

    match opts.mode.as_str() {
        "server" => run_server(opts).await,
        "client" => run_client(opts).await,
        mode => Err(boxed_error(format!("unknown mode {mode:?}"))),
    }
}

async fn run_server(opts: Options) -> BenchResult<()> {
    let _ = fs::remove_file(&opts.ready_file);
    let _ = fs::remove_file(&opts.start_file);

    let mut listener = KcpListener::bind(bench_config(), opts.addr).await?;
    let local_addr = listener.local_addr()?;

    let accepted = Arc::new(AtomicUsize::new(0));
    let echoes = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let payload_size = opts.payload_size;
    let accept_task = {
        let accepted = accepted.clone();
        let echoes = echoes.clone();
        let errors = errors.clone();
        tokio::spawn(async move {
            accept_echo(&mut listener, payload_size, &accepted, &echoes, &errors).await;
        })
    };

    write_file(&opts.ready_file, local_addr.to_string().as_bytes())?;
    println!(
        "server ready addr={local_addr} payload={} duration={} workers={}",
        opts.payload_size,
        format_duration(opts.duration),
        worker_count()
    );

    wait_file(&opts.start_file).await;

    let base_accepted = accepted.load(Ordering::Relaxed);
    let base_echoes = echoes.load(Ordering::Relaxed);
    let base_errors = errors.load(Ordering::Relaxed);
    println!("server benchmark start accepted={base_accepted} echoes={base_echoes} errors={base_errors}");

    time::sleep(opts.duration).await;

    let accepted_delta = accepted.load(Ordering::Relaxed) - base_accepted;
    let echo_delta = echoes.load(Ordering::Relaxed) - base_echoes;
    let error_delta = errors.load(Ordering::Relaxed) - base_errors;
    println!(
        "server benchmark complete accepted_delta={accepted_delta} echo_delta={echo_delta} error_delta={error_delta} echo_per_sec={:.1}",
        echo_delta as f64 / opts.duration.as_secs_f64()
    );

    accept_task.abort();
    Ok(())
}

async fn run_client(opts: Options) -> BenchResult<()> {
    wait_file(&opts.ready_file).await;

    let addr = fs::read_to_string(&opts.ready_file)
        .ok()
        .and_then(|addr| addr.trim().parse::<SocketAddr>().ok())
        .unwrap_or(opts.addr);

    let payload = Arc::new(vec![0x5a; opts.payload_size]);
    let clients = connect_and_warmup(addr, opts.sessions, payload.clone(), opts.timeout, opts.dial_parallel).await?;

    let start_stamp = now_stamp();
    write_file(&opts.start_file, start_stamp.as_bytes())?;
    println!(
        "client load start addr={addr} sessions={} payload={} interval={} duration={} timeout={} workers={}",
        opts.sessions,
        opts.payload_size,
        format_duration(opts.interval),
        format_duration(opts.duration),
        format_duration(opts.timeout),
        worker_count()
    );

    let (echoes, errors) = run_echo_load(clients, payload, opts.interval, opts.duration, opts.timeout).await?;
    println!(
        "client load complete client_echoes={echoes} client_errors={errors} client_echo_per_sec={:.1}",
        echoes as f64 / opts.duration.as_secs_f64()
    );

    Ok(())
}

async fn accept_echo(
    listener: &mut KcpListener,
    payload_size: usize,
    accepted: &AtomicUsize,
    echoes: &Arc<AtomicU64>,
    errors: &Arc<AtomicU64>,
) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(_) => return,
        };

        accepted.fetch_add(1, Ordering::Relaxed);

        let echoes = echoes.clone();
        let errors = errors.clone();
        tokio::spawn(async move {
            echo_session(stream, payload_size, &echoes, &errors).await;
        });
    }
}

async fn echo_session(mut stream: KcpStream, payload_size: usize, echoes: &AtomicU64, errors: &AtomicU64) {
    let mut buf = vec![0u8; payload_size];

    loop {
        if stream.read_exact(&mut buf).await.is_err() {
            return;
        }

        if stream.write_all(&buf).await.is_err() {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }

        echoes.fetch_add(1, Ordering::Relaxed);
    }
}

async fn connect_and_warmup(
    addr: SocketAddr,
    sessions: usize,
    payload: Arc<Vec<u8>>,
    timeout: Duration,
    parallel: usize,
) -> BenchResult<Vec<KcpStream>> {
    if parallel == 0 {
        return Err(boxed_error("dial-parallel must be > 0"));
    }

    let semaphore = Arc::new(Semaphore::new(parallel));
    let mut handles = Vec::with_capacity(sessions);

    for index in 0..sessions {
        let permit = semaphore.clone().acquire_owned().await?;
        let payload = payload.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit;
            warmup_one(addr, index, payload, timeout).await
        }));
    }

    let mut clients: Vec<Option<KcpStream>> = std::iter::repeat_with(|| None).take(sessions).collect();
    let mut echoed = 0usize;

    for handle in handles {
        let (index, stream) = handle.await??;
        clients[index] = Some(stream);
        echoed += 1;
    }

    if echoed != sessions {
        return Err(boxed_error(format!("warmup incomplete: got {echoed}, want {sessions}")));
    }

    clients
        .into_iter()
        .enumerate()
        .map(|(index, stream)| stream.ok_or_else(|| boxed_error(format!("nil client session at index {index}"))))
        .collect()
}

async fn warmup_one(
    addr: SocketAddr,
    index: usize,
    payload: Arc<Vec<u8>>,
    timeout: Duration,
) -> BenchResult<(usize, KcpStream)> {
    let config = bench_config();
    let mut stream = match time::timeout(timeout, KcpStream::connect(&config, addr)).await {
        Ok(Ok(stream)) => stream,
        Ok(Err(err)) => return Err(boxed_error(format!("warmup connect failed at session {index}: {err}"))),
        Err(_) => return Err(boxed_error(format!("warmup connect timeout at session {index}"))),
    };

    let mut reply = vec![0u8; payload.len()];
    let payload_for_io = payload.clone();
    let io_result = time::timeout(timeout, async {
        stream.write_all(payload_for_io.as_slice()).await?;
        stream.read_exact(&mut reply).await?;
        Ok::<(), io::Error>(())
    })
    .await;

    match io_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => return Err(boxed_error(format!("warmup I/O failed at session {index}: {err}"))),
        Err(_) => return Err(boxed_error(format!("warmup I/O timeout at session {index}"))),
    }

    if reply.as_slice() != payload.as_slice() {
        return Err(boxed_error(format!("warmup reply mismatch at session {index}")));
    }

    Ok((index, stream))
}

async fn run_echo_load(
    clients: Vec<KcpStream>,
    payload: Arc<Vec<u8>>,
    interval: Duration,
    duration: Duration,
    timeout: Duration,
) -> BenchResult<(u64, u64)> {
    let sessions = clients.len();
    let echoes = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let (stop_tx, _) = watch::channel(false);
    let mut handles = Vec::with_capacity(sessions);

    for (index, stream) in clients.into_iter().enumerate() {
        let payload = payload.clone();
        let echoes = echoes.clone();
        let errors = errors.clone();
        let stop_rx = stop_tx.subscribe();
        handles.push(tokio::spawn(async move {
            echo_load_session(
                index, sessions, stream, payload, interval, timeout, stop_rx, echoes, errors,
            )
            .await;
        }));
    }

    time::sleep(duration).await;
    let _ = stop_tx.send(true);

    for handle in handles {
        handle.await?;
    }

    Ok((echoes.load(Ordering::Relaxed), errors.load(Ordering::Relaxed)))
}

#[allow(clippy::too_many_arguments)]
async fn echo_load_session(
    index: usize,
    sessions: usize,
    mut stream: KcpStream,
    payload: Arc<Vec<u8>>,
    interval: Duration,
    timeout: Duration,
    mut stop_rx: watch::Receiver<bool>,
    echoes: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) {
    let mut reply = vec![0u8; payload.len()];
    let mut delay = Box::pin(time::sleep(first_delay(index, sessions, interval)));

    loop {
        tokio::select! {
            _ = stop_rx.changed() => return,
            _ = &mut delay => {}
        }

        let op_result = time::timeout(timeout, async {
            stream.write_all(payload.as_slice()).await?;
            stream.read_exact(&mut reply).await?;
            Ok::<(), io::Error>(())
        })
        .await;

        match op_result {
            Ok(Ok(())) if reply.as_slice() == payload.as_slice() => {
                echoes.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        delay = Box::pin(time::sleep(interval));
    }
}

fn parse_args() -> BenchResult<Options> {
    let mut opts = Options::default();
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        if arg == "-h" || arg == "--help" {
            print_usage();
            std::process::exit(0);
        }

        let (key, value) = split_flag(arg, &mut args)?;
        match key.as_str() {
            "mode" => opts.mode = value,
            "addr" => opts.addr = value.parse()?,
            "sessions" => opts.sessions = value.parse()?,
            "payload" => opts.payload_size = value.parse()?,
            "interval" => opts.interval = parse_duration_arg(&value)?,
            "duration" => opts.duration = parse_duration_arg(&value)?,
            "timeout" => opts.timeout = parse_duration_arg(&value)?,
            "dial-parallel" => opts.dial_parallel = value.parse()?,
            "ready-file" => opts.ready_file = value.into(),
            "start-file" => opts.start_file = value.into(),
            other => return Err(boxed_error(format!("unknown flag {other:?}"))),
        }
    }

    if opts.sessions == 0 {
        return Err(boxed_error("sessions must be > 0"));
    }

    if opts.payload_size == 0 {
        return Err(boxed_error("payload must be > 0"));
    }

    if opts.duration.is_zero() {
        return Err(boxed_error("duration must be > 0"));
    }

    Ok(opts)
}

fn split_flag(arg: String, args: &mut impl Iterator<Item = String>) -> BenchResult<(String, String)> {
    let flag = arg
        .strip_prefix("--")
        .or_else(|| arg.strip_prefix('-'))
        .ok_or_else(|| boxed_error(format!("expected flag, got {arg:?}")))?;

    if let Some((key, value)) = flag.split_once('=') {
        return Ok((key.to_owned(), value.to_owned()));
    }

    let value = args
        .next()
        .ok_or_else(|| boxed_error(format!("missing value for flag {flag:?}")))?;
    Ok((flag.to_owned(), value))
}

fn parse_duration_arg(value: &str) -> BenchResult<Duration> {
    if let Some(value) = value.strip_suffix("ms") {
        return Ok(Duration::from_millis(value.parse()?));
    }
    if let Some(value) = value.strip_suffix("us") {
        return Ok(Duration::from_micros(value.parse()?));
    }
    if let Some(value) = value.strip_suffix("ns") {
        return Ok(Duration::from_nanos(value.parse()?));
    }
    if let Some(value) = value.strip_suffix('s') {
        return Ok(Duration::from_secs(value.parse()?));
    }
    if let Some(value) = value.strip_suffix('m') {
        return Ok(Duration::from_secs(value.parse::<u64>()? * 60));
    }
    if let Some(value) = value.strip_suffix('h') {
        return Ok(Duration::from_secs(value.parse::<u64>()? * 60 * 60));
    }

    Ok(Duration::from_secs(value.parse()?))
}

fn first_delay(index: usize, sessions: usize, interval: Duration) -> Duration {
    assert!(sessions > 0, "sessions must be > 0");
    let nanos = interval.as_nanos() * index as u128 / sessions as u128;
    assert!(nanos <= u64::MAX as u128, "first delay too large");
    Duration::from_nanos(nanos as u64)
}

fn bench_config() -> KcpConfig {
    let mut config = KcpConfig::default();
    config.nodelay = KcpNoDelayConfig {
        nodelay: true,
        interval: 50,
        resend: 2,
        nc: false,
    };
    config.wnd_size = (128, 128);
    config.flush_write = true;
    config.flush_acks_input = false;
    config
}

fn write_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(path, contents)
}

async fn wait_file(path: &Path) {
    while fs::metadata(path).is_err() {
        time::sleep(Duration::from_millis(50)).await;
    }
}

fn now_stamp() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{}.{}", now.as_secs(), now.subsec_nanos())
}

fn worker_count() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0)
}

fn format_duration(duration: Duration) -> String {
    if duration.subsec_nanos() == 0 {
        return format!("{}s", duration.as_secs());
    }

    if duration.as_nanos() % 1_000_000 == 0 {
        return format!("{}ms", duration.as_millis());
    }

    if duration.as_nanos() % 1_000 == 0 {
        return format!("{}us", duration.as_micros());
    }

    format!("{}ns", duration.as_nanos())
}

fn boxed_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::new(io::ErrorKind::Other, message.into()))
}

fn print_usage() {
    println!(
        "serverbench --mode server|client --addr 127.0.0.1:40001 --sessions 5000 --payload 40 --interval 200ms --duration 20s --timeout 2s --dial-parallel 64 --ready-file target/profiles/serverbench.ready --start-file target/profiles/serverbench.start"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_delay_matches_kcp_go_linear_stagger() {
        let interval = Duration::from_millis(200);

        assert_eq!(first_delay(0, 5000, interval), Duration::ZERO);
        assert_eq!(first_delay(2500, 5000, interval), Duration::from_millis(100));
        assert_eq!(first_delay(4999, 5000, interval), Duration::from_nanos(199_960_000));
    }

    #[test]
    fn duration_parser_accepts_go_style_units() {
        assert_eq!(parse_duration_arg("200ms").unwrap(), Duration::from_millis(200));
        assert_eq!(parse_duration_arg("20s").unwrap(), Duration::from_secs(20));
    }
}
