//! The RAOP sender against a real AirPlay 1 receiver: shairport-sync,
//! writing what it plays to stdout. Ignored unless asked for, since it
//! needs the binary:
//!
//! ```sh
//! export TAS_SHAIRPORT_SYNC=$(nix-build '<nixpkgs>' -A shairport-sync --no-out-link)/bin/shairport-sync
//! cargo test -p tas-airplay-out --test shairport --no-run
//! unshare -rn sh -c 'ip link set lo up; cargo test -p tas-airplay-out --test shairport -- --ignored --nocapture'
//! ```
//!
//! shairport-sync 5.5 listens on 5000 whatever its `port` setting or `-p`
//! says, so the test runs in a network namespace of its own, where 5000 is
//! free even when a Music Assistant on the host holds it.

use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tas_airplay_out::rtp::NtpClock;
use tas_airplay_out::session::RaopSession;
use tas_airplay_out::{AirplayOutputs, RaopDevice};
use tas_audio::{SESSION_FORMAT, ServerClock, SineSource};
use tas_core::{PlayerCommand, PlayerRegistry};
use tas_sendspin::{SendspinServer, ServerConfig};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

/// Each read off stdout, with when it came.
type Reads = Arc<Mutex<Vec<(i64, Vec<u8>)>>>;

struct Shairport {
    _child: tokio::process::Child,
    _conf: tempfile::NamedTempFile,
    reads: Reads,
}

/// Starts shairport-sync on 127.0.0.1:5000, writing what it plays to
/// stdout, and returns once it takes connections.
async fn shairport(clock: ServerClock) -> Shairport {
    let binary = std::env::var("TAS_SHAIRPORT_SYNC").expect("TAS_SHAIRPORT_SYNC");
    // An empty config, so /etc/shairport-sync.conf isn't read.
    let conf = tempfile::NamedTempFile::new().unwrap();
    let mut child = Command::new(binary)
        .arg("-c")
        .arg(conf.path())
        .args(["-p", "5000", "-o", "stdout", "-m", "tinysvcmdns"])
        .args(["-a", "tas-airplay-out test", "-v"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let reads = Reads::default();
    let mut stdout = child.stdout.take().unwrap();
    tokio::spawn({
        let reads = reads.clone();
        async move {
            let mut buf = vec![0u8; 16 * 1024];
            while let Ok(n) = stdout.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                reads
                    .lock()
                    .unwrap()
                    .push((clock.now_us(), buf[..n].to_vec()));
            }
        }
    });
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(ADDR).await.is_ok() {
            return Shairport {
                _child: child,
                _conf: conf,
                reads,
            };
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("shairport-sync never took a connection");
}

impl Shairport {
    /// Frames written whose left sample is above an eighth of full scale,
    /// and when the first of them came.
    fn loud(&self) -> (usize, Option<i64>) {
        let reads = self.reads.lock().unwrap();
        let mut frames = 0;
        let mut first = None;
        for (at, bytes) in reads.iter() {
            let loud = bytes
                .as_chunks::<4>()
                .0
                .iter()
                .filter(|f| i16::from_le_bytes([f[0], f[1]]).abs() > 4_000)
                .count();
            if loud > 0 && first.is_none() {
                first = Some(*at);
            }
            frames += loud;
        }
        (frames, first)
    }
}

const ADDR: &str = "127.0.0.1:5000";

#[tokio::test]
#[ignore = "needs shairport-sync in TAS_SHAIRPORT_SYNC"]
async fn shairport_plays_what_we_send_when_we_say() {
    let clock = ServerClock::new();
    let ntp = NtpClock::new(clock);
    let receiver = shairport(clock).await;
    // The first connect can be reset while shairport finishes starting.
    let mut session = None;
    for _ in 0..20 {
        match RaopSession::connect(ADDR.parse().unwrap(), ntp, 0.0).await {
            Ok(s) => {
                session = Some(s);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    let mut session = session.expect("shairport-sync took no session");
    let at_us = clock.now_us() + 2_500_000;
    session.anchor(at_us).await.unwrap();
    // Three seconds of a 1 kHz sine at half scale.
    let samples: Vec<i16> = (0..44_100 * 3)
        .flat_map(|i| {
            let s =
                ((i as f64 * 1000.0 / 44_100.0 * std::f64::consts::TAU).sin() * 16_000.0) as i16;
            [s, s]
        })
        .collect();
    session.push(&samples).await.unwrap();
    session.finish().await.unwrap();
    tokio::time::sleep(Duration::from_millis(6_500)).await;
    session.teardown().await;

    let (frames_loud, first_loud) = receiver.loud();
    eprintln!(
        "{frames_loud} loud frames; the first {} ms after the anchor",
        first_loud.map_or(i64::MIN, |t| (t - at_us) / 1000)
    );
    // 84% of a sine at half scale is above an eighth of full scale.
    assert!(
        frames_loud > 44_100 * 3 * 4 / 5,
        "{frames_loud} loud frames"
    );
    let first = first_loud.expect("some audio");
    // The stdout backend has no DAC to wait for: it writes each frame as
    // it enters its 1 s output buffer ("audio backend desired buffer
    // length is 1.000000 seconds"), so the sine leaves it 0.75 s before
    // the anchor. That lead didn't move when the sync packets' latency
    // field was 1.5 s instead of 2, nor with the packets sent 1.5 s ahead:
    // shairport places frames by the sync packet's playing frame, as the
    // session assumes.
    assert!(
        (-1_000_000..=0).contains(&(first - at_us)),
        "the sine started {} ms from the anchor",
        (first - at_us) / 1000
    );
}

/// The whole path: a player in the registry, its Sendspin group, the
/// bridge and the session, to a real receiver.
#[tokio::test]
#[ignore = "needs shairport-sync in TAS_SHAIRPORT_SYNC"]
async fn a_bridged_player_plays_on_shairport() {
    let registry = Arc::new(PlayerRegistry::default());
    let server = SendspinServer::new(ServerConfig::new("tas", "Tune Assistant"), registry.clone());
    let clock = server.config().clock;
    let receiver = shairport(clock).await;
    let outputs = AirplayOutputs::with_local_addrs(server, vec![]);
    outputs.add(RaopDevice {
        player_id: "apshairport".into(),
        name: "shairport".into(),
        addr: ADDR.parse().unwrap(),
        model: Some("ShairportSync".into()),
        manufacturer: None,
        mac: None,
        fullname: "X@shairport._raop._tcp.local.".into(),
    });
    for _ in 0..500 {
        if registry.get("apshairport").is_some_and(|p| p.available) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    registry
        .execute("apshairport", PlayerCommand::VolumeSet(100))
        .await
        .unwrap();
    // Two seconds of a 48 kHz sine at half scale, in real time.
    let sine = SineSource::new(SESSION_FORMAT, 1000.0)
        .limit_frames(96_000)
        .paced(clock);
    registry
        .play_source("apshairport", Box::new(sine))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    outputs.shutdown().await;
    let (frames_loud, _) = receiver.loud();
    eprintln!("{frames_loud} loud frames");
    assert!(
        frames_loud > 44_100 * 2 * 3 / 4,
        "{frames_loud} loud frames"
    );
}
