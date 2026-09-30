//! Command-line and JSON configuration parsing.

use clap::Parser;
use serde::Deserialize;

fn normalize_go_alias(arg: std::ffi::OsString) -> std::ffi::OsString {
    match arg.to_str() {
        Some("-ds") => "--datashard".into(),
        Some("-ps") => "--parityshard".into(),
        _ => arg,
    }
}

/// Configuration struct matching the kcptun JSON config format.
///
/// Numeric fields match Go kcptun: time/duration fields that may be negative
/// (`autoexpire`, `scavengettl`, `keepalive`, `closewait`, `snmpperiod`) are
/// signed `i64`; count/window/size fields are unsigned and cannot be negative.
/// Negatives are clamped to zero when applied to the KCP/SMUX config.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Config {
    pub localaddr: Option<String>,
    pub remoteaddr: Option<String>,
    pub key: Option<String>,
    pub crypt: Option<String>,
    pub mode: Option<String>,
    pub conn: Option<u32>,
    pub autoexpire: Option<i64>,
    pub scavengettl: Option<i64>,
    pub mtu: Option<u32>,
    pub ratelimit: Option<u32>,
    pub sndwnd: Option<u32>,
    pub rcvwnd: Option<u32>,
    pub datashard: Option<u32>,
    pub parityshard: Option<u32>,
    pub dscp: Option<u32>,
    pub nocomp: Option<bool>,
    pub acknodelay: Option<bool>,
    pub nodelay: Option<u32>,
    pub interval: Option<u32>,
    pub resend: Option<u32>,
    pub nc: Option<u32>,
    pub sockbuf: Option<u32>,
    pub smuxver: Option<u8>,
    pub smuxbuf: Option<usize>,
    pub streambuf: Option<usize>,
    pub framesize: Option<usize>,
    pub keepalive: Option<i64>,
    pub keepalivetimeout: Option<i64>,
    pub ackstalltimeout: Option<i64>,
    pub closewait: Option<i64>,
    pub snmplog: Option<String>,
    pub snmpperiod: Option<i64>,
    pub log: Option<String>,
    pub quiet: Option<bool>,
    pub tcp: Option<bool>,
    pub pprof: Option<bool>,
    /// pprof bind address (same semantics as `--pprofaddr`).
    pub pprofaddr: Option<String>,
    #[cfg(feature = "qpp")]
    pub qpp: Option<bool>,
    #[cfg(feature = "qpp")]
    #[serde(rename = "qpp-count")]
    pub qppcount: Option<u16>,
}

/// kcptun client -- accelerate TCP over KCP.
#[derive(Debug, Parser)]
#[command(
    name = "kcptun-client",
    about,
    version,
    disable_version_flag = true,
    allow_negative_numbers = true
)]
pub(crate) struct Cli {
    /// Local listening address.
    #[arg(short = 'l', long)]
    pub localaddr: Option<String>,

    /// Remote server address.
    /// May be an IP (`1.2.3.4:29900`), a DNS hostname (`vps:29900`), or a
    /// multi-port range (`1.2.3.4:29900-29910`). Default matches Go kcptun.
    #[arg(short = 'r', long, default_value = "vps:29900")]
    pub remoteaddr: Option<String>,

    /// Pre-shared secret between client and server.
    #[arg(short, long, default_value = "it's a secrect", env = "KCPTUN_KEY")]
    pub key: Option<String>,

    /// Encryption method: null, none, xor, aes, aes-128, aes-192, aes-256,
    /// sm4, tea, xtea, salsa20, blowfish, twofish, cast5, 3des, aes-128-gcm.
    /// Only aes-128-gcm is authenticated — prefer it for production. The CFB
    /// family is CRC32-only (forgeable). xor/none/null are debug-only.
    #[arg(long, default_value = "aes")]
    pub crypt: Option<String>,

    /// Protocol mode: normal, fast, fast2, fast3.
    #[arg(short, long, default_value = "fast")]
    pub mode: Option<String>,

    /// Number of UDP connections to use (1–256). Values above 256 are
    /// rejected at startup: each conn is a session + task + socket.
    #[arg(long)]
    pub conn: Option<u32>,

    /// Auto-expire connections N seconds after session creation.
    #[arg(long)]
    pub autoexpire: Option<i64>,

    /// Scavenge TTL in seconds for expired connections.
    #[arg(long)]
    pub scavengettl: Option<i64>,

    /// MTU value.
    #[arg(long)]
    pub mtu: Option<u32>,

    /// Rate limit in bytes per second per connection (0 = disabled).
    #[arg(long, default_value_t = 0)]
    pub ratelimit: u32,

    /// Send window size.
    #[arg(long)]
    pub sndwnd: Option<u32>,

    /// Receive window size.
    #[arg(long)]
    pub rcvwnd: Option<u32>,

    /// FEC data shards.
    #[arg(long)]
    pub datashard: Option<u32>,

    /// FEC parity shards.
    #[arg(long)]
    pub parityshard: Option<u32>,

    /// DSCP value for IP packets.
    #[arg(long)]
    pub dscp: Option<u32>,

    /// Disable compression.
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub nocomp: bool,

    /// Enable ACK nodelay.
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub acknodelay: bool,

    /// Enable KCP nodelay.
    #[arg(long)]
    pub nodelay: Option<u32>,

    /// KCP update interval in ms.
    #[arg(long)]
    pub interval: Option<u32>,

    /// KCP fast resend threshold.
    #[arg(long)]
    pub resend: Option<u32>,

    /// KCP no congestion control flag.
    #[arg(long)]
    pub nc: Option<u32>,

    /// Socket buffer size in bytes.
    #[arg(long)]
    pub sockbuf: Option<u32>,

    /// SMUX protocol version (1 or 2).
    #[arg(long)]
    pub smuxver: Option<u8>,

    /// SMUX receive buffer size.
    #[arg(long)]
    pub smuxbuf: Option<usize>,

    /// SMUX stream buffer size.
    #[arg(long)]
    pub streambuf: Option<usize>,

    /// SMUX max frame size.
    #[arg(long)]
    pub framesize: Option<usize>,

    /// SMUX keepalive interval in seconds: how often a keepalive (NOP) frame is
    /// sent when the session is idle. 0 disables keepalives.
    #[arg(long, default_value = "10")]
    pub keepalive: Option<i64>,

    /// SMUX keepalive timeout in seconds: how long the session may go without a
    /// single inbound frame before it is declared dead (0 disables the check).
    /// Go keeps this at 30; on a lossy path a longer value rides out a transient
    /// dropout instead of tearing down every stream on the session.
    #[arg(long, default_value = "30")]
    pub keepalivetimeout: Option<i64>,

    /// Ack-stall timeout in seconds: how long outbound data may stay
    /// unacknowledged before the session is declared desynchronised. The fast
    /// path needs evidence that the peer restarted its KCP; without it only
    /// 60s of no ACK progress closes the session, so plain loss on the outbound
    /// path is left to KCP retransmission. 0 disables the check.
    #[arg(long, default_value = "10")]
    pub ackstalltimeout: Option<i64>,

    /// Close wait timeout in seconds.
    #[arg(long)]
    pub closewait: Option<i64>,

    /// SNMP log file path.
    #[arg(long)]
    pub snmplog: Option<String>,

    /// SNMP logging period in seconds.
    #[arg(long)]
    pub snmpperiod: Option<i64>,

    /// Log file path.
    #[arg(long)]
    pub log: Option<String>,

    /// Suppress log output.
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub quiet: bool,

    /// Use TCP instead of UDP for the underlying transport.
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub tcp: bool,

    /// Enable pprof HTTP server (binds `--pprofaddr`, loopback by default).
    #[arg(long, default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub pprof: bool,

    /// Bind address for the pprof HTTP server. Defaults to loopback: the
    /// profile endpoint exposes process internals and must not face the
    /// network. Use `--pprofaddr 0.0.0.0:6060` to restore Go kcptun's
    /// all-interfaces bind.
    #[arg(long, default_value = "127.0.0.1:6060")]
    pub pprofaddr: String,

    /// Enable QPP encryption.
    #[cfg(feature = "qpp")]
    #[arg(long = "QPP", default_value_t = false, action = clap::ArgAction::SetTrue)]
    pub qpp: bool,

    /// QPP pad count (should be prime).
    #[cfg(feature = "qpp")]
    #[arg(long = "QPPCount")]
    pub qppcount: Option<u16>,

    /// Path to JSON config file.
    #[arg(short = 'c', long)]
    pub c: Option<String>,

    /// Print version and exit (Go-compatible: `-v` / `--version`).
    #[arg(short = 'v', long = "version", action = clap::ArgAction::SetTrue, default_value_t = false)]
    pub version_flag: bool,
}

impl Cli {
    pub(crate) fn parse_go_compatible() -> Self {
        Self::parse_from(std::env::args_os().map(normalize_go_alias))
    }

    /// Merge CLI args with the JSON config taking precedence, matching Go.
    pub(crate) fn merge(cli: Self, cfg: Config) -> Self {
        Self {
            localaddr: cfg.localaddr.or(cli.localaddr),
            remoteaddr: cfg.remoteaddr.or(cli.remoteaddr),
            key: cfg.key.or(cli.key),
            crypt: cfg.crypt.or(cli.crypt),
            mode: cfg.mode.or(cli.mode),
            conn: cfg.conn.or(cli.conn),
            autoexpire: cfg.autoexpire.or(cli.autoexpire),
            scavengettl: cfg.scavengettl.or(cli.scavengettl),
            mtu: cfg.mtu.or(cli.mtu),
            ratelimit: cfg.ratelimit.unwrap_or(cli.ratelimit),
            sndwnd: cfg.sndwnd.or(cli.sndwnd),
            rcvwnd: cfg.rcvwnd.or(cli.rcvwnd),
            datashard: cfg.datashard.or(cli.datashard),
            parityshard: cfg.parityshard.or(cli.parityshard),
            dscp: cfg.dscp.or(cli.dscp),
            nocomp: cfg.nocomp.unwrap_or(cli.nocomp),
            acknodelay: cfg.acknodelay.unwrap_or(cli.acknodelay),
            nodelay: cfg.nodelay.or(cli.nodelay),
            interval: cfg.interval.or(cli.interval),
            resend: cfg.resend.or(cli.resend),
            nc: cfg.nc.or(cli.nc),
            sockbuf: cfg.sockbuf.or(cli.sockbuf),
            smuxver: cfg.smuxver.or(cli.smuxver),
            smuxbuf: cfg.smuxbuf.or(cli.smuxbuf),
            streambuf: cfg.streambuf.or(cli.streambuf),
            framesize: cfg.framesize.or(cli.framesize),
            keepalive: cfg.keepalive.or(cli.keepalive),
            keepalivetimeout: cfg.keepalivetimeout.or(cli.keepalivetimeout),
            ackstalltimeout: cfg.ackstalltimeout.or(cli.ackstalltimeout),
            closewait: cfg.closewait.or(cli.closewait),
            snmplog: cfg.snmplog.or(cli.snmplog),
            snmpperiod: cfg.snmpperiod.or(cli.snmpperiod),
            log: cfg.log.or(cli.log),
            quiet: cfg.quiet.unwrap_or(cli.quiet),
            tcp: cfg.tcp.unwrap_or(cli.tcp),
            pprof: cfg.pprof.unwrap_or(cli.pprof),
            pprofaddr: cfg.pprofaddr.unwrap_or(cli.pprofaddr),
            #[cfg(feature = "qpp")]
            qpp: cfg.qpp.unwrap_or(cli.qpp),
            #[cfg(feature = "qpp")]
            qppcount: cfg.qppcount.or(cli.qppcount),
            c: cli.c,
            version_flag: false,
        }
    }
}

#[cfg(test)]
mod tests {

    /// M-6 rework: client JSON must reject unknown fields too.
    #[test]
    fn client_json_rejects_unknown_fields() {
        assert!(
            serde_json::from_str::<Config>(r#"{"mmtu":1350}"#).is_err(),
            "misspelled client fields must fail fast"
        );
        assert!(serde_json::from_str::<Config>(r#"{"mtu":1350}"#).is_ok());
    }

    use super::*;

    #[test]
    fn json_false_overrides_cli_true_and_unknown_fields_are_rejected() {
        let cli = Cli::try_parse_from(["kcptun-client", "--tcp", "--nocomp", "--quiet", "--pprof"])
            .unwrap();
        // M-6 rework: deny_unknown_fields — a typo like "mmtu" must fail
        // at startup instead of being silently ignored.
        assert!(
            serde_json::from_str::<Config>(r#"{"mmtu":1350}"#).is_err(),
            "misspelled client fields must fail fast"
        );
        let cfg: Config =
            serde_json::from_str(r#"{"tcp":false,"nocomp":false,"quiet":false,"pprof":false}"#)
                .unwrap();

        let merged = Cli::merge(cli, cfg);
        assert!(!merged.tcp);
        assert!(!merged.nocomp);
        assert!(!merged.quiet);
        assert!(!merged.pprof);
    }

    /// pprofaddr: JSON overrides CLI default (loopback).
    #[test]
    fn json_pprofaddr_overrides_cli_default() {
        let cli = Cli::try_parse_from(["kcptun-client", "-r", "1.2.3.4:29900"]).unwrap();
        let cfg: Config = serde_json::from_str(r#"{"pprofaddr": "10.0.0.1:7070"}"#).unwrap();
        let merged = Cli::merge(cli, cfg);
        assert_eq!(merged.pprofaddr, "10.0.0.1:7070");

        let cli = Cli::try_parse_from(["kcptun-client", "-r", "1.2.3.4:29900"]).unwrap();
        let cfg: Config = serde_json::from_str(r#"{}"#).unwrap();
        let merged = Cli::merge(cli, cfg);
        assert_eq!(merged.pprofaddr, "127.0.0.1:6060");
    }

    #[test]
    fn go_fec_aliases_are_accepted() {
        let args = ["kcptun-client", "-ds", "4", "-ps", "2"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .map(normalize_go_alias);
        let cli = Cli::try_parse_from(args).unwrap();
        assert_eq!(cli.datashard, Some(4));
        assert_eq!(cli.parityshard, Some(2));
    }
}
