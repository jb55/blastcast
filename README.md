# blastcast

One timeline across every audio protocol.

Every protocol has its own clock (RAOP's NTP, AirPlay 2's PTP, Sendspin's
time sync) and its own way of saying "this frame plays then". blastcast puts
them all on one clock, the `ServerClock`. Each input maps its sender's clock
to it with a `ClockMap` and says when its frames play with an `Anchor`, so
any input can feed any output.

It is a cargo workspace, one crate per directory under `crates/`:

| crate | what it holds |
|---|---|
| `blastcast` | the timing core: the clock, clock maps, anchors, timed sources, the rate match and the timeline |
| `blastcast-airplay-out` | AirPlay 1 output: a native RAOP sender (RTSP, ALAC, RTP, NTP timing, resends), discovery of `_raop._tcp` speakers, and the bridge that makes each speaker a Sendspin player (`AirplayOutputs`); a fake RAOP speaker in `testing` |
| `blastcast-audio` | audio plumbing between protocols: PCM sample conversion (`pcm`), a streaming resampler over rubato (`StreamResampler`, `Resampled`, `TimedResampled`, `to_session_format`) and a test tone (`SineSource`) |
| `blastcast-mdns` | one mDNS responder shared by every protocol: our adverts (any `Service`, with `_sendspin-server._tcp` and `_mass._tcp` built in) and browsing for players |
| `blastcast-telemetry` | every timing a protocol measures: the `Series` (with its `Kind` and `Unit`), `Point` and the status types (`Stream`, `Receiver`, `Session`, `Speaker`), serde only; and behind the default `hub` feature, on tokio, the hub (`Telemetry`, `Probe`), a minute of history per series streamed live, and `Roster`, a watch of a protocol's entities |
| `blastcast-sendspin` | the Sendspin server: handshakes, clock sync, groups, streams (Opus, FLAC, PCM) and pairing. An app hears about its players through the `Host` trait and keeps pairings with `Trust`; behind the `testing` feature, an in-memory speaker for other crates' tests |

The AirPlay receiver (`blastcast-airplay`) is moving in beside them from
tune-assistant-server.

`blastcast-mdns`'s `advertise_browse` test advertises and browses over the
host's real interfaces, so `./scripts/check.sh` needs multicast on the
machine running it.
`blastcast-sendspin`'s tests decode its Opus with libopus, which the `opus`
crate builds from source, so they need cmake.
`blastcast-airplay-out`'s `tests/shairport.rs` plays to a real
shairport-sync and is ignored unless asked for; its header says how.

The `blastcast` crate's modules:

| module | what it holds |
|---|---|
| `clock` | `ServerClock`: microseconds since start, on tokio's (pausable) `Instant` |
| `format` | `AudioFormat`, `PcmChunk` and the session format |
| `map` | `ClockMap` (offset, drift, error against the server clock) and `Anchor` (frame F plays at remote time T, at rate R) |
| `source` | `LiveSource`, and `TimedSource` yielding `Item::{Chunk, Flush, End}` |
| `rate` | `RateMatch`: absorbs the drift between a sender's clock and ours, measured against stamps |
| `timeline` | `Timeline` and `Span`: the server-clock time of each chunk from the frames before it, exact over any run |

The `blastcast` crate knows no protocol and no server. The protocol crates
depend on it, never the reverse. Each protocol keeps its own clock estimator
and hands it plain data.

Its first user is
[tune-assistant-server](https://github.com/jb55/tune-assistant-server),
whose RAOP and AirPlay 2 receivers are timed sources and whose Sendspin
stream is the sink. It depends on these crates by path, as a sibling checkout
(`../blastcast/crates/<name>`).

## Checking

```sh
./scripts/check.sh   # fmt, clippy with warnings denied, tests, over the workspace
```

The history before this repo existed is that of each crate's directory in
tune-assistant-server (`crates/blastcast`, `crates/tas-mdns`, `crates/tas-audio`,
`crates/tas-telemetry`, `crates/tas-sendspin` until it became
`crates/blastcast-sendspin`, and `crates/tas-airplay-out`), carried over
with `git subtree split`.
