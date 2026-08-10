use std::{
    env, fs,
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{Duration, timeout},
};
use tokio_kcp::{KcpListener, KcpStream};

const DATA_SHARDS: usize = 2;
const PARITY_SHARDS: usize = 1;

#[tokio::test]
async fn kcp_go_server_echoes_rust_client_with_fec() {
    let _ = env_logger::try_init();

    let Some(kcp_go_path) = kcp_go_path() else {
        return;
    };

    let helper = GoHelper::new(&kcp_go_path);
    let mut server = helper.spawn_server();
    let server_addr = read_server_addr(&mut server).parse::<SocketAddr>().unwrap();

    let mut stream = KcpStream::dial_with_options(server_addr, DATA_SHARDS, PARITY_SHARDS)
        .await
        .unwrap();
    stream.write_all(b"rust-to-go").await.unwrap();
    stream.flush().await.unwrap();

    let mut buffer = [0u8; 64];
    let n = timeout(Duration::from_secs(5), stream.read(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buffer[..n], b"rust-to-go");

    let _ = server.kill();
}

#[tokio::test]
async fn rust_server_echoes_kcp_go_client_with_fec() {
    let _ = env_logger::try_init();

    let Some(kcp_go_path) = kcp_go_path() else {
        return;
    };

    let mut listener = KcpListener::listen_with_options("127.0.0.1:0", DATA_SHARDS, PARITY_SHARDS)
        .await
        .unwrap();
    let server_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 64];
        let n = stream.read(&mut buffer).await.unwrap();
        stream.write_all(&buffer[..n]).await.unwrap();
        stream.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let helper = GoHelper::new(&kcp_go_path);
    let output = tokio::task::spawn_blocking(move || helper.run_client(server_addr))
        .await
        .unwrap();
    assert_eq!(output.trim(), "go-to-rust");
}

fn kcp_go_path() -> Option<PathBuf> {
    env::var_os("KCP_GO_PATH").map(PathBuf::from).or_else(|| {
        PathBuf::from(r"D:\kcp-go")
            .is_dir()
            .then(|| PathBuf::from(r"D:\kcp-go"))
    })
}

struct GoHelper {
    dir: PathBuf,
}

impl GoHelper {
    fn new(kcp_go_path: &Path) -> GoHelper {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let dir = PathBuf::from("target").join(format!("kcp-go-interop-{nanos}"));
        fs::create_dir_all(&dir).unwrap();

        let kcp_go_path = kcp_go_path.to_string_lossy().replace('\\', "/");
        fs::write(
            dir.join("go.mod"),
            format!(
                "module tokio-kcp-interop\n\ngo 1.24\n\nrequire github.com/xtaci/kcp-go/v5 v5.0.0\n\nreplace github.com/xtaci/kcp-go/v5 => {kcp_go_path}\n"
            ),
        )
        .unwrap();
        fs::write(dir.join("main.go"), GO_HELPER).unwrap();

        GoHelper { dir }
    }

    fn spawn_server(&self) -> Child {
        Command::new("go")
            .args(["run", "-mod=mod", ".", "server"])
            .current_dir(&self.dir)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap()
    }

    fn run_client(&self, addr: SocketAddr) -> String {
        let output = Command::new("go")
            .args(["run", "-mod=mod", ".", "client", &addr.to_string()])
            .current_dir(&self.dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "go client failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
}

fn read_server_addr(server: &mut Child) -> String {
    let stdout = server.stdout.take().unwrap();
    let mut line = String::new();
    BufReader::new(stdout).read_line(&mut line).unwrap();
    line.trim().to_owned()
}

const GO_HELPER: &str = r#"
package main

import (
	"fmt"
	"io"
	"os"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

const dataShards = 2
const parityShards = 1

func main() {
	switch os.Args[1] {
	case "server":
		server()
	case "client":
		client(os.Args[2])
	default:
		panic("bad mode")
	}
}

func server() {
	l, err := kcp.ListenWithOptions("127.0.0.1:0", nil, dataShards, parityShards)
	if err != nil {
		panic(err)
	}
	defer l.Close()

	fmt.Println(l.Addr().String())

	conn, err := l.Accept()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		panic(err)
	}
	if _, err = conn.Write(buf[:n]); err != nil {
		panic(err)
	}
	time.Sleep(200 * time.Millisecond)
}

func client(addr string) {
	conn, err := kcp.DialWithOptions(addr, nil, dataShards, parityShards)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err = conn.Write([]byte("go-to-rust")); err != nil {
		panic(err)
	}

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		panic(err)
	}
	fmt.Print(string(buf[:n]))
}
"#;
