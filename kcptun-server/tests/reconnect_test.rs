//! Functional test: multi-conn mode and server-restart reconnection.
//!
//! Unlike the stress_test which uses `shutdown(Write)` (broken pre-existing
//! echo behavior in base code), this test avoids half-close and verifies
//! correctness via full-duplex echo with explicit wire patterns.
//!
//! // Usage:
//! //   cargo test --release -p kcptun-server --test reconnect_test -- --nocapture

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Newest modification time among the `src/` trees of the workspace crates
/// under `root`.
fn newest_source_mtime(root: &std::path::Path) -> Option<std::time::SystemTime> {
    fn scan(dir: &std::path::Path, newest: &mut Option<std::time::SystemTime>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                scan(&path, newest);
            } else if path.extension().is_some_and(|e| e == "rs") {
                if let Ok(t) = entry.metadata().and_then(|m| m.modified()) {
                    if newest.is_none_or(|cur| t > cur) {
                        *newest = Some(t);
                    }
                }
            }
        }
    }

    let mut newest = None;
    for entry in std::fs::read_dir(root).ok()?.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        if !path.is_dir() || name.to_string_lossy().starts_with('.') || name == "target" {
            continue;
        }
        let src = path.join("src");
        if src.is_dir() {
            scan(&src, &mut newest);
        }
    }
    newest
}

/// True when `path` is at least as new as the workspace sources it should have
/// been built from (or when that cannot be determined).
fn binary_is_fresh(path: &str) -> bool {
    let bin_time = std::fs::metadata(path).and_then(|m| m.modified()).ok();
    // <root>/target/<profile>/<bin>: the workspace root is three levels up.
    let root = std::path::Path::new(path)
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.parent());
    let Some((bin_time, src_time)) = bin_time.zip(root.and_then(newest_source_mtime)) else {
        return true;
    };
    bin_time >= src_time
}

/// Resolve a workspace binary.
///
/// `cargo test -p kcptun-server` does NOT rebuild the kcptun-client binary, so
/// an existing-but-stale `target/release/kcptun-client` would silently put
/// pre-change code under test (this hid a client compile error once). Prefer
/// the first candidate that is newer than the sources; if every candidate is
/// stale, say so loudly and use it anyway.
fn find_bin(name: &str) -> String {
    let mut first_existing = None;
    for dir in &[
        "target/release",
        "target/debug",
        "../target/release",
        "../target/debug",
    ] {
        let path = format!("{}/{}", dir, name);
        if !std::path::Path::new(&path).exists() {
            continue;
        }
        if binary_is_fresh(&path) {
            return path;
        }
        first_existing.get_or_insert(path);
    }
    match first_existing {
        Some(stale) => {
            eprintln!(
                "WARNING: {stale} is older than the workspace sources it should have been \
                 built from — this run may be testing stale code. Run `make release` (or \
                 `cargo build --release`) first."
            );
            stale
        }
        None => name.to_string(),
    }
}

fn kill_port(port: u16) {
    let _ = Command::new("sh")
        .arg("-c")
        .arg(format!("lsof -ti:{} | xargs kill -9 2>/dev/null", port))
        .output();
}

struct ReconnectEnv {
    procs: Vec<Child>,
    cli_port: u16,
    srv_port: u16,
    target_port: u16,
    crypt: String,
    nocomp: bool,
    keepalive: u64,
    mode: String,
}

impl ReconnectEnv {
    fn start(
        target_port: u16,
        srv_port: u16,
        cli_port: u16,
        crypt: &str,
        nocomp: bool,
        conn: usize,
        keepalive: u64,
        mode: &str,
    ) -> Self {
        for p in &[target_port, srv_port, cli_port] {
            kill_port(*p);
        }
        thread::sleep(Duration::from_millis(800));

        // TCP echo server (single-shot echo, no shutdown dependency)
        let echo = Command::new("python3")
            .arg("-u")
            .arg("-c")
            .arg(format!(
                "import socket,threading as _t\ndef _h(c):\n d=c.recv(65536)\n if d:c.sendall(d)\n c.close()\n\
                 s=socket.socket();s.setsockopt(65535,4,1);s.bind(('',{}));s.listen(128)\n\
                 while 1:_t.Thread(target=_h,args=(s.accept()[0],)).start()",
                target_port
            ))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("echo");
        thread::sleep(Duration::from_millis(500));

        let mut srv_args: Vec<String> = vec![
            "-t".into(),
            format!("127.0.0.1:{}", target_port),
            "-l".into(),
            format!(":{}", srv_port),
            "--key".into(),
            "k".into(),
            "--crypt".into(),
            crypt.into(),
            "--mode".into(),
            mode.into(),
            "--datashard".into(),
            "0".into(),
            "--parityshard".into(),
            "0".into(),
            "--sndwnd".into(),
            "2048".into(),
            "--rcvwnd".into(),
            "2048".into(),
            "--keepalive".into(),
            keepalive.to_string(),
        ];
        if nocomp {
            srv_args.push("--nocomp".into());
        }

        let sv = Command::new(&find_bin("kcptun-server"))
            .args(&srv_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("srv");
        thread::sleep(Duration::from_secs(1));

        let mut cli_args: Vec<String> = vec![
            "-r".into(),
            format!("127.0.0.1:{}", srv_port),
            "-l".into(),
            format!(":{}", cli_port),
            "--key".into(),
            "k".into(),
            "--crypt".into(),
            crypt.into(),
            "--mode".into(),
            mode.into(),
            "--datashard".into(),
            "0".into(),
            "--parityshard".into(),
            "0".into(),
            "--sndwnd".into(),
            "2048".into(),
            "--rcvwnd".into(),
            "2048".into(),
            "--keepalive".into(),
            keepalive.to_string(),
            "--conn".into(),
            conn.to_string(),
        ];
        if nocomp {
            cli_args.push("--nocomp".into());
        }

        let cl = Command::new(&find_bin("kcptun-client"))
            .args(&cli_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("cli");
        thread::sleep(Duration::from_secs(2));

        ReconnectEnv {
            procs: vec![echo, sv, cl],
            cli_port,
            srv_port,
            target_port,
            crypt: crypt.to_string(),
            nocomp,
            keepalive,
            mode: mode.to_string(),
        }
    }

    fn send_echo(&self, msg: &[u8]) -> Option<Vec<u8>> {
        let mut s = TcpStream::connect(format!("127.0.0.1:{}", self.cli_port)).ok()?;
        s.set_read_timeout(Some(Duration::from_secs(4))).ok();
        s.write_all(msg).ok()?;
        thread::sleep(Duration::from_millis(150));
        let mut resp = Vec::with_capacity(msg.len());
        let mut buf = [0u8; 65536];
        loop {
            match s.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    resp.extend_from_slice(&buf[..n]);
                    if resp.len() >= msg.len() {
                        break;
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break
                }
                Err(_) => break,
            }
        }
        if resp.is_empty() {
            None
        } else {
            Some(resp)
        }
    }

    /// Fast probe: send data without waiting for response.
    /// Used to drive retransmit traffic when the server is dead.
    fn probe_send(&self, msg: &[u8]) {
        if let Ok(mut s) = TcpStream::connect(format!("127.0.0.1:{}", self.cli_port)) {
            let _ = s.set_write_timeout(Some(Duration::from_secs(1)));
            let _ = s.write_all(msg);
        }
    }

    fn kill_server(&mut self) {
        if self.procs.len() > 1 {
            let _ = self.procs[1].kill();
            let _ = self.procs[1].wait();
        }
    }

    fn restart_server(&mut self) {
        // Kill only the server Child. Do NOT kill_port(srv_port): the client's
        // connected UDP sockets also match `lsof -ti:PORT`, so kill -9 would
        // take down the client (the old test restarted the client to hide this).
        self.kill_server();
        thread::sleep(Duration::from_millis(500));

        let mut srv_args: Vec<String> = vec![
            "-t".into(),
            format!("127.0.0.1:{}", self.target_port),
            "-l".into(),
            format!(":{}", self.srv_port),
            "--key".into(),
            "k".into(),
            "--crypt".into(),
            self.crypt.clone(),
            "--mode".into(),
            self.mode.clone(),
            "--datashard".into(),
            "0".into(),
            "--parityshard".into(),
            "0".into(),
            "--sndwnd".into(),
            "2048".into(),
            "--rcvwnd".into(),
            "2048".into(),
            "--keepalive".into(),
            self.keepalive.to_string(),
        ];
        if self.nocomp {
            srv_args.push("--nocomp".into());
        }

        let sv = Command::new(&find_bin("kcptun-server"))
            .args(&srv_args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("srv restart");
        if self.procs.len() > 1 {
            self.procs[1] = sv;
        } else {
            self.procs.push(sv);
        }
        thread::sleep(Duration::from_secs(1));
        if let Some(cli) = self.procs.get_mut(2) {
            match cli.try_wait() {
                Ok(Some(status)) => println!("  WARNING: client exited early: {status}"),
                Ok(None) => println!("  client still running"),
                Err(e) => println!("  client wait err: {e}"),
            }
        }
    }
}

impl Drop for ReconnectEnv {
    fn drop(&mut self) {
        for p in &mut self.procs {
            let _ = p.kill();
        }
    }
}

/// Generate a deterministic payload.
fn make_payload(seed: usize, size: usize) -> Vec<u8> {
    (0..size)
        .map(|i| (seed as u8).wrapping_add(i as u8))
        .collect()
}

/// --conn 4: all connections pass data (round-robin).
#[test]
fn test_multi_conn_baseline() {
    let e = ReconnectEnv::start(19700, 39700, 19701, "null", true, 4, 30, "fast");
    let mut ok = 0usize;
    let mut fail = 0usize;
    for i in 0..24 {
        let payload = make_payload(i, 2048);
        match e.send_echo(&payload) {
            Some(resp) if resp == payload => ok += 1,
            _ => fail += 1,
        }
    }
    drop(e);
    assert!(ok >= 20, "multi-conn: only {ok}/24 ok, {fail} fail");
    println!("✅ multi-conn baseline OK (--conn 4: {ok}/24)")
}

/// --conn 4 reconnect after server restart: kill, probe, restart, verify recovery.
#[test]
fn test_reconnect_after_restart() {
    let conn_n = 4;
    let base = 19600 + (std::process::id() % 1000) as u16;
    // Use mode "fast" (not fast3) — fast3 + --conn 4 has baseline timeout issues.
    let mut e = ReconnectEnv::start(base, base + 1, base + 2, "null", true, conn_n, 2, "fast");

    // Baseline
    for i in 0..8 {
        let p = make_payload(100 + i, 512);
        assert!(
            e.send_echo(&p).as_deref() == Some(&p[..]),
            "baseline conn {i} failed"
        );
    }
    println!("  baseline OK");

    // Kill server
    e.kill_server();
    println!("  server killed");

    // Probes to drive dead_link — fast send without waiting for response
    // (send_echo would block 8s per call on the dead server).
    for _ in 0..40 {
        let p = make_payload(200, 64);
        e.probe_send(&p);
        thread::sleep(Duration::from_millis(200));
    }
    println!("  probes done (~8s of retransmit traffic)");

    // Restart server only — the client must redial in-process (clear old KCP
    // state + new UDP socket), matching the production server-restart path.
    // Do NOT restart the client: that would hide a broken reconnect path.
    e.restart_server();
    println!("  server restarted (client kept running)");

    // Give the client a moment to observe fatal UDP errors / keepalive and
    // replace dead pool slots, then probe until echo recovers.
    // Default SMUX keepalive timeout is 30s — allow that plus reconnect.
    thread::sleep(Duration::from_millis(500));

    let mut consecutive = 0usize;
    let start = Instant::now();
    for i in 0..150 {
        let p = make_payload(300 + i, 256);
        match e.send_echo(&p) {
            Some(resp) if resp == p => {
                if consecutive == 0 {
                    println!("  first recovery OK at t+{:?}", start.elapsed());
                }
                consecutive += 1;
                if consecutive >= 8 {
                    break;
                }
            }
            _ => {
                consecutive = 0;
                thread::sleep(Duration::from_millis(200));
            }
        }
    }
    println!(
        "  recovery overall: {}/150 after {:?}",
        consecutive,
        start.elapsed()
    );
    drop(e);
    assert!(
        consecutive >= 8,
        "reconnect recovery failed: only {consecutive} consecutive OK (need 8). \
         Client should redial in-process after server restart without a client restart."
    );
    println!("✅ multi-conn reconnect OK (--conn 4, {consecutive} consecutive)");
}
