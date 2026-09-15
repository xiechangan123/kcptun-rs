//! Full-lifecycle test for session retirement (`--autoexpire` replacement).
//!
//! Proves all three phases from one log-verified run:
//!
//!   1. **The retired session keeps serving** — a long download that starts on
//!      the old session keeps receiving data across the replacement (its pipe
//!      completes with the full payload *after* the reconnect line).
//!   2. **New connections use the new session** — the short request issued
//!      after the replacement is accepted by the server from a *different*
//!      client source port than the download (one UDP socket per session).
//!   3. **The retired session is closed only after it finished serving** — the
//!      scavenger's retirement line appears *after* the download's
//!      "pipe completed" line, and the session is really gone afterwards.
//!
//! Usage:
//!   cargo build --release -p kcptun-client -p kcptun-server
//!   cargo test --release -p kcptun-server --test session_retirement_test -- --nocapture

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const TARGET_PORT: u16 = 19710;
const SRV_PORT: u16 = 19711;
const CLI_PORT: u16 = 19712;
const KEY: &str = "retire-test";
/// Payload the long download must receive in full.
const BIG_BYTES: usize = 12 * 1024 * 1024;
/// `--autoexpire`: the pool slot is replaced once a new connection lands after
/// this many seconds.
const AUTOEXPIRE_SECS: u64 = 6;
/// The retired session's own TTL deadline is `autoexpire + scavengettl`, so a
/// small TTL keeps the close inside the test window.
const SCAVENGETTL_SECS: u64 = 8;

fn find_bin(name: &str) -> String {
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let workspace_root = std::path::Path::new(&manifest_dir)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        for profile in ["release", "debug"] {
            let path = workspace_root.join(format!("target/{profile}")).join(name);
            if path.exists() {
                return path.to_string_lossy().to_string();
            }
        }
    }
    name.to_string()
}

fn kill_port(port: u16) {
    let _ = Command::new("sh")
        .arg("-c")
        .arg(format!("lsof -ti:{} | xargs kill -9 2>/dev/null", port))
        .output();
}

/// Collect a child's output lines into a shared log (order preserved).
fn tee_lines(child: &mut Child, log: Arc<Mutex<Vec<String>>>) {
    for stream in [
        child
            .stdout
            .take()
            .map(|s| Box::new(s) as Box<dyn Read + Send>),
        child
            .stderr
            .take()
            .map(|s| Box::new(s) as Box<dyn Read + Send>),
    ]
    .into_iter()
    .flatten()
    {
        let log = log.clone();
        thread::spawn(move || {
            for line in BufReader::new(stream).lines().map_while(Result::ok) {
                println!("    | {line}");
                log.lock().unwrap().push(line);
            }
        });
    }
}

fn log_index(lines: &[String], needle: &str) -> Option<usize> {
    lines.iter().position(|l| l.contains(needle))
}

fn log_count(lines: &[String], needle: &str) -> usize {
    lines.iter().filter(|l| l.contains(needle)).count()
}

/// Bytes the client reports receiving on a `pipe completed: N sent, M recv` line.
fn recv_bytes_of(line: &str) -> usize {
    line.split("recv")
        .next()
        .and_then(|head| head.trim_end().rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

struct TestEnv {
    procs: Vec<Child>,
    cli_log: Arc<Mutex<Vec<String>>>,
    srv_log: Arc<Mutex<Vec<String>>>,
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        for p in &mut self.procs {
            let _ = p.kill();
            let _ = p.wait();
        }
    }
}

#[test]
fn retired_session_serves_new_traffic_then_closes() {
    for p in [TARGET_PORT, SRV_PORT, CLI_PORT] {
        kill_port(p);
    }
    thread::sleep(Duration::from_millis(500));

    // Slow HTTP target: /big streams BIG_BYTES at ~1.5 MiB/s (so the download
    // spans the autoexpire window), /small answers immediately.
    let target_code = format!(
        r#"
import socket, threading as t, time
BIG = {BIG_BYTES}
def handle(c):
    try:
        req = c.recv(4096).decode(errors='ignore')
        path = req.split(' ')[1] if ' ' in req else '/'
        if path.startswith('/big'):
            c.sendall(b"HTTP/1.0 200 OK\r\nContent-Length: %d\r\n\r\n" % BIG)
            sent = 0
            while sent < BIG:
                n = min(65536, BIG - sent)
                c.sendall(b'b' * n); sent += n
                time.sleep(0.04)
        else:
            body = b'small-ok'
            c.sendall(b"HTTP/1.0 200 OK\r\nContent-Length: %d\r\n\r\n" % len(body) + body)
    except OSError:
        pass
    finally:
        c.close()
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', {TARGET_PORT})); s.listen(64)
while 1:
    t.Thread(target=handle, args=(s.accept()[0],), daemon=True).start()
"#
    );
    let target = Command::new("python3")
        .arg("-u")
        .arg("-c")
        .arg(target_code)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("target");
    thread::sleep(Duration::from_millis(400));

    let srv_log = Arc::new(Mutex::new(Vec::new()));
    let mut server = Command::new(find_bin("kcptun-server"))
        .args([
            "-l",
            &format!(":{SRV_PORT}"),
            "-t",
            &format!("127.0.0.1:{TARGET_PORT}"),
            "--key",
            KEY,
            "--crypt",
            "null",
            "--mode",
            "fast3",
            "--mtu",
            "1350",
            "--datashard",
            "0",
            "--parityshard",
            "0",
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("server");
    tee_lines(&mut server, srv_log.clone());
    thread::sleep(Duration::from_millis(800));

    let cli_log = Arc::new(Mutex::new(Vec::new()));
    let mut client = Command::new(find_bin("kcptun-client"))
        .args([
            "-l",
            &format!("127.0.0.1:{CLI_PORT}"),
            "-r",
            &format!("127.0.0.1:{SRV_PORT}"),
            "--key",
            KEY,
            "--crypt",
            "null",
            "--mode",
            "fast3",
            "--mtu",
            "1350",
            "--datashard",
            "0",
            "--parityshard",
            "0",
            "--conn",
            "1",
            "--autoexpire",
            &AUTOEXPIRE_SECS.to_string(),
            "--scavengettl",
            &SCAVENGETTL_SECS.to_string(),
            "--sockbuf",
            "4194304",
            "--smuxbuf",
            "4194304",
            "--streambuf",
            "262144",
        ])
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("client");
    tee_lines(&mut client, cli_log.clone());
    thread::sleep(Duration::from_millis(1200));

    let env = TestEnv {
        procs: vec![target, server, client],
        cli_log: cli_log.clone(),
        srv_log: srv_log.clone(),
    };

    // ── Phase 1 begins: a long download lands on the initial session ────────
    println!("[1] starting the long download ({BIG_BYTES} bytes)");
    let progress = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    let download_started = Instant::now();
    {
        let progress = progress.clone();
        let done = done.clone();
        thread::spawn(move || {
            let mut sock = TcpStream::connect(("127.0.0.1", CLI_PORT)).expect("connect big");
            sock.set_read_timeout(Some(Duration::from_secs(60)))
                .unwrap();
            sock.write_all(b"GET /big HTTP/1.0\r\n\r\n")
                .expect("send big");
            let mut buf = [0u8; 65536];
            let mut total = 0usize;
            loop {
                match sock.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        total += n;
                        progress.store(total, Ordering::Release);
                        if total >= BIG_BYTES + 64 {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            done.store(total, Ordering::Release);
        });
    }

    // Let the download flow, then wait past the autoexpire deadline.
    thread::sleep(Duration::from_secs(AUTOEXPIRE_SECS + 1));

    // Bytes already delivered to the *retired* session when the replacement
    // happens — anything after this proves the old session kept serving.
    let bytes_before_replacement = progress.load(Ordering::Acquire);
    println!(
        "[1] download has received {} bytes before the replacement",
        bytes_before_replacement
    );

    // ── Phase 2: a new connection triggers the replacement ─────────────────
    println!("[2] new connection after the expiry window -> must use the new session");
    let mut small = TcpStream::connect(("127.0.0.1", CLI_PORT)).expect("connect small");
    small
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    small
        .write_all(b"GET /small HTTP/1.0\r\n\r\n")
        .expect("send small");
    let mut small_body = String::new();
    let _ = small.read_to_string(&mut small_body);
    drop(small);
    println!("[2] small request done: {:?}", small_body.trim_end());

    // Give the retirement deadline (`autoexpire + scavengettl`) room to pass,
    // plus one scavenger tick (5s).
    let deadline = download_started + Duration::from_secs(AUTOEXPIRE_SECS + SCAVENGETTL_SECS + 12);
    while Instant::now() < deadline && done.load(Ordering::Acquire) < BIG_BYTES {
        thread::sleep(Duration::from_millis(250));
    }
    thread::sleep(Duration::from_secs(8)); // let the scavenger retire the old session

    let received = done.load(Ordering::Acquire);
    let cli = env.cli_log.lock().unwrap().clone();
    let srv = env.srv_log.lock().unwrap().clone();
    println!("\n===== CLIENT LOG =====");
    for l in &cli {
        println!("{l}");
    }
    println!("\n===== SERVER LOG =====");
    for l in &srv {
        println!("{l}");
    }

    // ── Phase 1: the retired session kept serving the download ─────────────
    assert!(
        received >= BIG_BYTES,
        "download incomplete: {received}/{BIG_BYTES} bytes"
    );
    let reconnect_at = log_index(&cli, "reconnecting").expect("no reconnect line in client log");
    let completed_at = cli
        .iter()
        .position(|l| l.contains("pipe completed:") && recv_bytes_of(l) >= BIG_BYTES)
        .expect("no download-completion line in client log");
    assert!(
        completed_at > reconnect_at,
        "download completed before the replacement — nothing was proven\nclient log:\n{}",
        cli.join("\n")
    );
    assert!(
        progress.load(Ordering::Acquire) > bytes_before_replacement,
        "download made no progress after the replacement"
    );

    // ── Phase 2: the new connection was served by the *new* session ────────
    // One UDP socket per session, so the server sees a different source port
    // for the post-replacement connection.
    let accepting: Vec<&String> = srv
        .iter()
        .filter(|l| l.contains("accepting stream"))
        .collect();
    assert!(
        accepting.len() >= 2,
        "expected at least two accepted streams on the server, got {}",
        accepting.len()
    );
    let port_of = |line: &str| -> String {
        line.split("from ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .unwrap_or_default()
            .to_string()
    };
    let download_peer = port_of(accepting[0]);
    let post_peer = port_of(accepting[accepting.len() - 1]);
    assert_ne!(
        download_peer,
        post_peer,
        "post-replacement connection arrived on the same session socket\nserver log:\n{}",
        srv.join("\n")
    );

    // ── Phase 3: the retired session was closed only after it finished ─────
    let retired_at = log_index(&cli, "scavenger: session retired")
        .expect("scavenger never retired the old session — it was not closed");
    assert!(
        retired_at > completed_at,
        "the old session was retired before its stream finished\nclient log:\n{}",
        cli.join("\n")
    );
    let retired_line = cli[retired_at].clone();
    assert!(
        retired_line.contains("0 stream(s) done"),
        "retirement happened with streams still attached: {retired_line}"
    );
    assert_eq!(
        log_count(&cli, "scavenger: session normally closed"),
        0,
        "retired session should be closed by the ttl path, not as dead"
    );

    println!("\n✅ all three phases verified:");
    println!("   1. retired session kept serving  (download {received} B, completed after the reconnect)");
    println!("   2. new connection used the new session ({download_peer} -> {post_peer})");
    println!("   3. old session closed after serving: {retired_line}");
}
