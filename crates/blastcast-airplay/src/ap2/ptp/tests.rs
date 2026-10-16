use std::time::Duration;

use super::*;
use crate::ap2::testing::Grandmaster;

const PHONE: u64 = 0x0011_22ff_fe33_4455;
const HOMEPOD: u64 = 0x66aa_bbff_fecc_ddee;
const TV: u64 = 0x74d0_82ff_fe72_6d91;

fn ip(s: &str) -> IpAddr {
    s.parse().unwrap()
}

/// A follower whose sockets nobody sends to: the tests feed it packets
/// with the server time they were read at.
async fn follower(telemetry: Telemetry) -> Ptp {
    let any = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let ptp = Ptp::bind_to(ServerClock::new(), telemetry, any, any).await;
    assert!(
        matches!(ptp.status(), Status::Running { .. }),
        "{:?}",
        ptp.status()
    );
    ptp
}

/// A grandmaster `offset` µs ahead of the server clock at 0, gaining
/// `ppm`: its time at server time `server_us`, in ns.
fn remote_ns(offset: i64, ppm: f64, server_us: i64) -> i128 {
    let us = server_us + offset + (server_us as f64 * ppm * 1e-6).round() as i64;
    i128::from(us) * 1000
}

#[tokio::test]
async fn sync_and_follow_up_pairs_map_a_known_offset_and_drift() {
    let telemetry = Telemetry::new();
    let ptp = follower(telemetry.clone()).await;
    let phone = ip("192.168.1.20");
    let follow = ptp.follow([phone]).unwrap();
    let mut gm = Grandmaster::new(PHONE);
    // An Apple clock: seconds since the phone booted.
    let (offset, ppm) = (86_400_123_456_789, 41.5);
    ptp.receive(&gm.announce(), phone, 0);
    let mut server = 1_000_000;
    for i in 0..400u64 {
        let (sync, follow_up) = gm.sync(remote_ns(offset, ppm, server));
        // 50 to 450 µs on the way, a quick one every fourth.
        let delay = if i.is_multiple_of(4) {
            1
        } else {
            50 + (i * 7919 % 400) as i64
        };
        let read = server + delay;
        // The two sockets race: every third Follow_Up beats its Sync.
        if i.is_multiple_of(3) {
            ptp.receive(&follow_up, phone, read + 300);
            ptp.receive(&sync, phone, read);
        } else {
            ptp.receive(&sync, phone, read);
            ptp.receive(&follow_up, phone, read + 300);
        }
        server += 125_000;
    }
    let (id, map) = follow.map().expect("a map");
    assert_eq!(id, PHONE);
    assert_eq!(ptp.map(PHONE), Some(map));
    assert!((map.drift_ppm - ppm).abs() < 0.2, "{map:?}");
    for at in (1_000_000..server).step_by(999_983) {
        let want = (remote_ns(offset, ppm, at) / 1000) as i64;
        let got = map.to_remote_us(at);
        assert!((got - want).abs() <= 5, "at {at}: off by {}", got - want);
    }

    let history = telemetry.subscribe(server).history;
    let track = |s| {
        history
            .iter()
            .find(|t| t.series == s && &*t.entity == "ptp:001122fffe334455")
            .unwrap_or_else(|| panic!("no {s:?}"))
    };
    assert_eq!(track(Series::PtpOffset).points.len(), 400);
    let drift = track(Series::PtpDrift).points.last().unwrap().value;
    assert!((drift - ppm).abs() < 0.2, "{drift}");
    assert!(track(Series::PtpError).points.last().unwrap().value < 10.0);
    assert_eq!(track(Series::Grandmaster).points.len(), 1);
}

#[tokio::test]
async fn a_better_grandmaster_takes_the_session_over() {
    let telemetry = Telemetry::new();
    let ptp = follower(telemetry.clone()).await;
    let (phone, homepod) = (ip("192.168.1.20"), ip("fe80::1"));
    let follow = ptp.follow([phone]).unwrap();
    let mut gm = Grandmaster::new(PHONE);
    let mut pod = Grandmaster::new(HOMEPOD);
    pod.announce.priority1 = 128;
    ptp.receive(&gm.announce(), phone, 0);
    let (sync, follow_up) = gm.sync(5_000_000_000);
    ptp.receive(&sync, phone, 10);
    ptp.receive(&follow_up, phone, 20);
    assert_eq!(follow.map().map(|m| m.0), Some(PHONE));

    // The HomePod isn't the session's peer yet: it is dropped.
    ptp.receive(&pod.announce(), homepod, 30);
    assert_eq!(ptp.report().ignored, 1);
    assert_eq!(follow.grandmaster(), Some(PHONE));

    // SETPEERS adds it, and it announces a better clock.
    follow.set_peers([phone, homepod]);
    ptp.receive(&pod.announce(), homepod, 40);
    let (sync, follow_up) = pod.sync(9_000_000_000);
    ptp.receive(&sync, homepod, 50);
    ptp.receive(&follow_up, homepod, 60);
    let (id, map) = follow.map().unwrap();
    assert_eq!(id, HOMEPOD);
    assert_eq!(map.offset_us, 9_000_000 - 50);
    // The phone's map is still there, for an anchor on its timeline.
    assert_eq!(ptp.map(PHONE).unwrap().offset_us, 5_000_000 - 10);

    let report = ptp.report();
    let changes: Vec<_> = report.changes.iter().map(|c| (c.from, c.to)).collect();
    assert_eq!(changes, [(None, Some(PHONE)), (Some(PHONE), Some(HOMEPOD))]);
    assert_eq!(report.follows, [(vec![phone, homepod], Some(HOMEPOD))]);
    let marks = telemetry.subscribe(100).history;
    assert!(
        marks
            .iter()
            .any(|t| t.series == Series::Grandmaster && &*t.entity == "ptp:66aabbfffeccddee")
    );

    // The HomePod falls silent: once its Announce goes stale, the phone's
    // is the best fresh one.
    let later = 40 + ANNOUNCE_FRESH_US + 1;
    ptp.receive(&gm.announce(), phone, later);
    assert_eq!(report_gm(&ptp), Some(PHONE));
    drop(follow);
    assert!(ptp.report().follows.is_empty());
}

/// A TV's clock wins, and its Syncs don't reach us: the phone follows it
/// and sends the TV's time in its own Syncs, under its own clock id, with
/// Announces naming the TV. The anchors name the TV, so that is the map.
#[tokio::test]
async fn a_peer_relaying_another_grandmaster_maps_that_timeline() {
    let ptp = follower(Telemetry::default()).await;
    let (phone, tv) = (ip("192.168.1.84"), ip("192.168.1.103"));
    let follow = ptp.follow([phone, tv]).unwrap();
    let mut gm = Grandmaster::new(PHONE);
    ptp.receive(&gm.announce(), phone, 0);
    let (sync, follow_up) = gm.sync(5_000_000_000);
    ptp.receive(&sync, phone, 10);
    ptp.receive(&follow_up, phone, 20);
    assert_eq!(ptp.map(PHONE).unwrap().offset_us, 5_000_000 - 10);

    // The TV wins: the phone announces it, steps removed, and its Syncs
    // carry the TV's time from now on.
    gm.announce.priority1 = 128;
    gm.announce.grandmaster = TV;
    gm.announce.steps_removed = 1;
    ptp.receive(&gm.announce(), phone, 100_000);
    assert_eq!(follow.grandmaster(), Some(TV));
    assert_eq!(follow.map(), None, "no Sync on the TV's time yet");
    let tv_offset = 84_161_000_000;
    let mut server = 125_000;
    for _ in 0..8 {
        let (sync, follow_up) = gm.sync(i128::from(server + tv_offset) * 1000);
        ptp.receive(&sync, phone, server + 40);
        ptp.receive(&follow_up, phone, server + 90);
        server += 125_000;
    }
    let (id, map) = follow
        .map()
        .expect("the TV's timeline, from the phone's Syncs");
    assert_eq!(id, TV);
    assert_eq!(map.offset_us, tv_offset - 40);
    assert_eq!(ptp.map(TV), Some(map));
    // Its Syncs aren't on the phone's own timeline now.
    assert_eq!(ptp.map(PHONE), None);
    let report = ptp.report();
    let clock = report.clocks.iter().find(|c| c.id == PHONE).unwrap();
    assert_eq!((clock.timeline, clock.steps), (TV, 0), "a restart, no step");

    // Should the TV's own Syncs reach us, they are the better map.
    let mut own = Grandmaster::new(TV);
    own.announce.priority1 = 128;
    ptp.receive(&own.announce(), tv, server);
    let (sync, follow_up) = own.sync(i128::from(server + tv_offset) * 1000);
    ptp.receive(&sync, tv, server + 5);
    ptp.receive(&follow_up, tv, server + 6);
    assert_eq!(ptp.map(TV).unwrap().offset_us, tv_offset - 5);

    // The TV goes quiet while the phone carries on relaying its time: the
    // relay's live map, not the TV's frozen one.
    for _ in 0..48 {
        server += 125_000;
        let (sync, follow_up) = gm.sync(i128::from(server + tv_offset) * 1000);
        ptp.receive(&sync, phone, server + 40);
        ptp.receive(&follow_up, phone, server + 90);
    }
    assert_eq!(ptp.map(TV).unwrap().offset_us, tv_offset - 40);

    // The phone takes the timeline back, and its Syncs are its own again.
    gm.announce = Grandmaster::new(PHONE).announce;
    ptp.receive(&gm.announce(), phone, server + 10);
    let (sync, follow_up) = gm.sync(7_000_000_000);
    ptp.receive(&sync, phone, server + 20);
    ptp.receive(&follow_up, phone, server + 30);
    assert_eq!(ptp.map(PHONE).unwrap().offset_us, 7_000_000 - server - 20);
}

fn report_gm(ptp: &Ptp) -> Option<u64> {
    ptp.report().follows.first()?.1
}

#[tokio::test]
async fn without_an_announce_the_syncing_clock_is_grandmaster() {
    let ptp = follower(Telemetry::default()).await;
    let phone = ip("10.0.0.2");
    let follow = ptp.follow([phone]).unwrap();
    assert_eq!(follow.grandmaster(), None);
    let mut gm = Grandmaster::new(PHONE);
    let (sync, follow_up) = gm.sync(1_000_000_000);
    ptp.receive(&sync, phone, 0);
    assert_eq!(follow.map(), None, "half a pair is no sample");
    ptp.receive(&follow_up, phone, 0);
    assert_eq!(report_gm(&ptp), Some(PHONE));
}

#[tokio::test]
async fn a_taken_port_runs_without_timing() {
    let taken = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = taken.local_addr().unwrap();
    let free = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let ptp = Ptp::bind_to(ServerClock::new(), Telemetry::default(), addr, free).await;
    let Status::Unavailable(why) = ptp.status() else {
        panic!("{:?}", ptp.status())
    };
    assert!(why.contains(&format!("port {}", addr.port())), "{why}");
    let refused = ptp.follow([ip("10.0.0.2")]).unwrap_err();
    assert_eq!(&refused, why);
    assert_eq!(ptp.map(PHONE), None);
    assert!(Ptp::off().follow([]).is_err());
}

/// The sockets: a grandmaster on loopback, sending as a phone does.
#[tokio::test]
async fn follows_a_grandmaster_over_udp() {
    let any = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let clock = ServerClock::new();
    let ptp = Ptp::bind_to(clock, Telemetry::default(), any, any).await;
    let Status::Running { event, general } = *ptp.status() else {
        panic!("{:?}", ptp.status())
    };
    let follow = ptp.follow([ip("127.0.0.1")]).unwrap();
    let socket = tokio::net::UdpSocket::bind(any).await.unwrap();
    let mut gm = Grandmaster::new(PHONE);
    let offset_us = 123_456_789_000;
    let map = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let sent = i128::from(clock.now_us() + offset_us) * 1000;
            gm.send(&socket, event, general, sent, true).await.unwrap();
            if let Some(map) = follow.map() {
                return map;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a map within 5 s");
    assert_eq!(map.0, PHONE);
    // Loopback is quick: within 5 ms.
    let got = map.1.offset_us - offset_us;
    assert!((-5_000..=0).contains(&got), "{got}");
}

#[test]
fn peers_come_from_timing_peer_info_and_setpeers() {
    use plist::{Dictionary, Value};
    let addresses = Value::Array(vec![
        Value::String("192.168.1.20".into()),
        Value::String("fe80::1c2b:3a4d:5e6f:7081%en0".into()),
        Value::String("not an address".into()),
    ]);
    let mut info = Dictionary::new();
    info.insert("Addresses".into(), addresses.clone());
    info.insert("ID".into(), Value::String("192.168.1.20".into()));
    let want = [ip("192.168.1.20"), ip("fe80::1c2b:3a4d:5e6f:7081")];
    assert_eq!(peer_addresses(&Value::Dictionary(info.clone())), want);
    // SETPEERS: an array of addresses; SETPEERSX: of timingPeerInfos.
    assert_eq!(peer_addresses(&addresses), want);
    let mut other = Dictionary::new();
    other.insert(
        "Addresses".into(),
        Value::Array(vec![Value::String("::ffff:10.0.0.3".into())]),
    );
    let x = Value::Array(vec![Value::Dictionary(info), Value::Dictionary(other)]);
    let mut all = want.to_vec();
    all.push(ip("10.0.0.3"));
    assert_eq!(peer_addresses(&x), all);
}
