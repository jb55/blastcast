//! A sine tone sent to an AirPlay 1 receiver, as a phone would play to it:
//! for watching a receiver (tas' own, on its dashboard) without a phone.
//!
//! ```sh
//! cargo run -p blastcast-airplay-out --example tone-sender -- 127.0.0.1:7000 [seconds] [hz]
//! ```
//!
//! It keeps its own clock, so the receiver's clock map has a real offset
//! to find, anchors two and a half seconds ahead, and queues no more than
//! three seconds of audio ahead of now.

use std::net::SocketAddr;
use std::time::Duration;

use blastcast::ServerClock;
use blastcast_airplay_out::{NtpClock, RaopSession};

const RATE: i64 = 44_100;
const CHUNK: usize = 352;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let usage = "usage: tone-sender <host:port> [seconds] [hz]";
    let addr: SocketAddr = args.next().expect(usage).parse().expect(usage);
    let seconds: f64 = args.next().map_or(60.0, |s| s.parse().expect(usage));
    let hz: f64 = args.next().map_or(440.0, |s| s.parse().expect(usage));

    let clock = ServerClock::new();
    let mut session = RaopSession::connect(addr, NtpClock::new(clock), -15.0)
        .await
        .expect("connect");
    let start = clock.now_us() + 2_500_000;
    session.anchor(start).await.expect("anchor");

    let total = (seconds * RATE as f64) as i64;
    let mut sent = 0i64;
    while sent < total {
        // No more than three seconds ahead of now.
        let ahead = start + sent * 1_000_000 / RATE - clock.now_us();
        if ahead > 3_000_000 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let samples: Vec<i16> = (0..CHUNK)
            .flat_map(|i| {
                let t = (sent + i as i64) as f64 / RATE as f64;
                let s = ((t * hz * std::f64::consts::TAU).sin() * 6000.0) as i16;
                [s, s]
            })
            .collect();
        session.push(&samples).await.expect("push");
        sent += CHUNK as i64;
    }
    session.finish().await.expect("finish");
    // Let the last of it play before saying goodbye.
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    session.teardown().await;
}
