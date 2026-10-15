//! The RAOP sender against the fake speaker: the RTSP sequence, packets,
//! sync, timing, resends, flush, refusals and hang-ups.

use std::time::Duration;

use blastcast::ServerClock;
use blastcast_airplay_out::alac::FRAMES_PER_PACKET;
use blastcast_airplay_out::rtp::NtpClock;
use blastcast_airplay_out::session::{LATENCY_FRAMES, RaopSession, SessionError};
use blastcast_airplay_out::testing::FakeSpeaker;

const WAIT: Duration = Duration::from_secs(5);

async fn connect(speaker: &FakeSpeaker) -> (RaopSession, NtpClock) {
    let ntp = NtpClock::new(ServerClock::new());
    let session = RaopSession::connect(speaker.addr(), ntp, -15.0)
        .await
        .unwrap();
    (session, ntp)
}

fn ramp(frames: usize) -> Vec<i16> {
    (0..frames * 2)
        .map(|i| (i as i32 % 30_000) as i16)
        .collect()
}

#[tokio::test]
async fn sets_up_like_itunes() {
    let speaker = FakeSpeaker::start().await;
    let (session, _) = connect(&speaker).await;
    assert_eq!(session.audio_latency(), Some(11_025));
    let log = speaker.log();
    assert_eq!(
        log.methods(),
        ["OPTIONS", "ANNOUNCE", "SETUP", "RECORD", "SET_PARAMETER"]
    );
    let announce = &log.requests[1];
    assert_eq!(announce.header("Content-Type"), Some("application/sdp"));
    assert!(announce.body.contains("a=rtpmap:96 AppleLossless\r\n"));
    assert!(
        announce
            .body
            .contains("a=fmtp:96 352 0 16 40 10 14 2 255 0 0 44100\r\n")
    );
    assert!(announce.body.contains("c=IN IP4 127.0.0.1\r\n"));
    let setup = &log.requests[2];
    assert!(
        setup
            .header("Transport")
            .unwrap()
            .starts_with("RTP/AVP/UDP;unicast;interleaved=0-1;mode=record;control_port=")
    );
    assert_eq!(setup.header("Session"), None);
    let record = &log.requests[3];
    assert_eq!(
        record.header("Session"),
        Some("DEADBEEF"),
        "the id, not the timeout"
    );
    assert_eq!(record.header("Range"), Some("npt=0-"));
    assert!(record.header("RTP-Info").unwrap().starts_with("seq="));
    assert_eq!(log.requests[4].body, "volume: -15.000000\r\n");
    // Every request names the same sender.
    let ids: Vec<_> = log.requests.iter().map(|r| r.header("DACP-ID")).collect();
    assert!(ids.iter().all(|id| id.is_some() && *id == ids[0]));
    let cseqs: Vec<_> = log
        .requests
        .iter()
        .map(|r| r.header("CSeq").unwrap())
        .collect();
    assert_eq!(cseqs, ["1", "2", "3", "4", "5"]);
}

#[tokio::test]
async fn sends_packets_on_the_anchored_timeline() {
    let speaker = FakeSpeaker::start().await;
    let (mut session, ntp) = connect(&speaker).await;
    let record = speaker.log().requests[3]
        .header("RTP-Info")
        .unwrap()
        .to_owned();
    let (seq0, rtp0) = rtp_info(&record);
    assert_eq!(session.next_rtp(), rtp0);

    let at_us = ntp.server_clock().now_us() + 2_000_000;
    session.anchor(at_us).await.unwrap();
    let samples = ramp(1000);
    session.push(&samples).await.unwrap();
    assert_eq!(session.next_rtp(), rtp0.wrapping_add(1000));
    session.finish().await.unwrap();
    speaker
        .wait_for("three packets and a sync", WAIT, |l| {
            l.packets.len() == 3 && !l.syncs.is_empty()
        })
        .await;

    let log = speaker.log();
    let frames: Vec<usize> = log.packets.iter().map(|p| p.samples.len() / 2).collect();
    assert_eq!(frames, [FRAMES_PER_PACKET, FRAMES_PER_PACKET, 296]);
    for (i, p) in log.packets.iter().enumerate() {
        assert_eq!(p.header.seq, seq0.wrapping_add(i as u16));
        assert_eq!(p.header.timestamp, rtp0.wrapping_add(i as u32 * 352));
        assert_eq!(p.header.marker, i == 0, "only the first is marked");
    }
    assert_eq!(log.samples(), samples);
    let sync = log.syncs[0];
    assert!(sync.first);
    assert_eq!(sync.sending.wrapping_sub(sync.playing), LATENCY_FRAMES);
    drop(log);
    // The first frame plays at the anchor, to within a frame's rounding.
    let first = speaker.play_time_us(&ntp, rtp0).unwrap();
    assert!(
        (first - at_us).abs() <= 23,
        "plays at {first}, anchored at {at_us}"
    );
}

#[tokio::test]
async fn answers_timing_and_resend_requests() {
    let speaker = FakeSpeaker::start().await;
    let (mut session, ntp) = connect(&speaker).await;
    speaker.ask_time(0x1234_5678_9abc_def0).await;
    speaker
        .wait_for("a timing reply", WAIT, |l| !l.timing_replies.is_empty())
        .await;
    let (origin, received, sent) = speaker.log().timing_replies[0];
    assert_eq!(origin, 0x1234_5678_9abc_def0);
    let now = ntp.now();
    assert!(received <= now && sent <= now);
    assert!(ntp.us_at(now) - ntp.us_at(received) < 1_000_000);

    session.anchor(ntp.server_clock().now_us()).await.unwrap();
    session.push(&ramp(352 * 3)).await.unwrap();
    speaker
        .wait_for("three packets", WAIT, |l| l.packets.len() == 3)
        .await;
    let first = speaker.log().packets[0].header.seq;
    speaker.ask_resend(first.wrapping_add(1), 5).await;
    speaker
        .wait_for("two resent", WAIT, |l| {
            l.packets.iter().filter(|p| p.resent).count() == 2
        })
        .await;
    let log = speaker.log();
    let resent: Vec<_> = log.packets.iter().filter(|p| p.resent).collect();
    assert_eq!(resent[0].header.seq, first.wrapping_add(1));
    assert_eq!(resent[1].header.seq, first.wrapping_add(2));
    assert_eq!(resent[0].samples, log.packets[1].samples);
}

#[tokio::test]
async fn a_flush_restarts_the_stream_where_it_was() {
    let speaker = FakeSpeaker::start().await;
    let (mut session, ntp) = connect(&speaker).await;
    session.anchor(ntp.server_clock().now_us()).await.unwrap();
    session.push(&ramp(400)).await.unwrap();
    speaker
        .wait_for("the first packet", WAIT, |l| l.packets.len() == 1)
        .await;
    let next = session.next_rtp();
    session.flush().await.unwrap();
    assert_eq!(session.current_anchor(), None);
    assert_eq!(session.next_rtp(), next, "the 48 queued frames are dropped");
    session.anchor(ntp.server_clock().now_us()).await.unwrap();
    session.push(&ramp(352)).await.unwrap();
    speaker
        .wait_for("two packets", WAIT, |l| l.packets.len() == 2)
        .await;
    let log = speaker.log();
    let flush = log.requests.iter().find(|r| r.method == "FLUSH").unwrap();
    let (seq, rtp) = rtp_info(flush.header("RTP-Info").unwrap());
    let after = &log.packets[1];
    assert_eq!((after.header.seq, after.header.timestamp), (seq, rtp));
    assert_eq!(rtp, next);
    assert!(
        after.header.marker,
        "the first packet after a flush is marked"
    );
    assert_eq!(log.syncs.iter().filter(|s| s.first).count(), 2);
}

#[tokio::test]
async fn packets_go_two_seconds_before_they_play() {
    let speaker = FakeSpeaker::start().await;
    let (mut session, ntp) = connect(&speaker).await;
    let at_us = ntp.server_clock().now_us() + 2_300_000;
    session.anchor(at_us).await.unwrap();
    session.push(&ramp(352 * 2)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(speaker.log().packets.is_empty(), "not due for 300 ms");
    speaker
        .wait_for("both packets", WAIT, |l| l.packets.len() == 2)
        .await;
    let sent = ntp.server_clock().now_us();
    assert!(
        sent >= at_us - 2_000_000,
        "sent at {sent}, plays at {at_us}"
    );

    // A flush drops what wasn't sent yet.
    session.push(&ramp(352 * 4)).await.unwrap();
    session.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(speaker.log().packets.len(), 2);
}

#[tokio::test]
async fn refusals_say_why() {
    let busy = FakeSpeaker::start().await;
    busy.refuse("ANNOUNCE", 453);
    let ntp = NtpClock::new(ServerClock::new());
    let err = RaopSession::connect(busy.addr(), ntp, 0.0)
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::Busy), "{err}");

    let locked = FakeSpeaker::start().await;
    locked.refuse("OPTIONS", 401);
    let err = RaopSession::connect(locked.addr(), ntp, 0.0)
        .await
        .unwrap_err();
    assert!(matches!(err, SessionError::Auth(401)), "{err}");
}

#[tokio::test]
async fn notices_when_the_speaker_hangs_up() {
    let speaker = FakeSpeaker::start().await;
    let (mut session, _) = connect(&speaker).await;
    let closed = tokio::spawn(async move { session.closed().await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!closed.is_finished());
    speaker.hang_up();
    tokio::time::timeout(WAIT, closed).await.unwrap().unwrap();
}

#[tokio::test]
async fn tears_down() {
    let speaker = FakeSpeaker::start().await;
    let (session, _) = connect(&speaker).await;
    session.teardown().await;
    assert_eq!(speaker.log().methods().last(), Some(&"TEARDOWN"));
}

fn rtp_info(info: &str) -> (u16, u32) {
    let get = |key: &str| {
        info.split(';')
            .find_map(|p| p.strip_prefix(&format!("{key}=")))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    (get("seq") as u16, get("rtptime") as u32)
}
