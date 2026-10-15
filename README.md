# blastcast

Synchronized multi-protocol audio, protocols included: take `blastcast`,
`blastcast-airplay` and `blastcast-sendspin` and get any input playing in
sync on any mix of outputs.

Every protocol has its own clock (RAOP's NTP, AirPlay 2's PTP, Sendspin's
time sync) and its own way of saying "this frame plays then". blastcast puts
them all on one clock, the `ServerClock`. Each input maps its sender's clock
to it with a `ClockMap` and says when its frames play with an `Anchor`, so
any input can feed any output.

## The relay: AirPlay in, every Sendspin player out

`blastcast-relay` is the whole idea in one binary: an AirPlay receiver a
phone plays to, and a Sendspin server every speaker on the network plays
from, as one group, at the phone's time.

```sh
cargo run --release -p blastcast-relay -- --name Relay
```

The phone shows **Relay** as an AirPlay speaker. Sendspin players find the
relay by its `_sendspin-server._tcp` advert, and it dials the ones that
advertise `_sendspin._tcp`. Every available player joins one group, a late
one too, and plays what the phone sends: pause, seek, the volume slider,
and what is playing (title, artist, cover art, progress) for players that
show it.

| flag | default | |
|---|---|---|
| `--name` | `blastcast` | what the phone and the players show |
| `--airplay-port` | 7000 | the receiver's RTSP port |
| `--sendspin-port` | 8927 | the Sendspin server's port |
| `--airplay2` | off | advertise AirPlay 2 too; its PTP timing binds ports 319 and 320, so grant the binary `cap_net_bind_service` |
| `--state-dir` | `$XDG_STATE_HOME/blastcast-relay` | where `relay.json` keeps the identities a phone and a paired speaker know it by |
| `--mdns-interface` | every interface | the interface (or address) to advertise on |

Each flag also reads `BLASTCAST_RELAY_<FLAG>` from the environment. A
speaker plays from one server at a time, so stop any other Sendspin server
(Music Assistant, tune-assistant-server) that would dial it back.

It depends on nothing but this workspace: `cargo tree -p blastcast-relay`
names no app crate. `crates/blastcast-relay/tests/relay.rs` runs it end to
end on localhost, a RAOP phone into it and Sendspin speakers out: the first
frame plays at the same instant on every speaker, at the phone's time
(less the lead an AirPlay 1 phone pads it with, which nothing plays to).

## The crates

It is a cargo workspace, one crate per directory under `crates/`:

| crate | what it holds |
|---|---|
| `blastcast` | the timing core: the clock, clock maps, anchors, timed sources, the rate match and the timeline |
| `blastcast-airplay` | AirPlay input: the native RAOP receiver (`raop`), AirPlay 2 with pairing, FairPlay setup and PTP (`ap2`), each receiver kept running (`receiver`), and the sender's audio as a timed source with its metadata. An app supplies the AirPlay 2 identity's storage; behind the `testing` feature, an AirPlay 2 sender for other crates' tests |
| `blastcast-airplay-out` | AirPlay 1 output: a native RAOP sender (RTSP, ALAC, RTP, NTP timing, resends), discovery of `_raop._tcp` speakers, and the bridge that makes each speaker a Sendspin player (`AirplayOutputs`); a fake RAOP speaker in `testing` |
| `blastcast-audio` | audio plumbing between protocols: PCM sample conversion (`pcm`), a streaming resampler over rubato (`StreamResampler`, `Resampled`, `TimedResampled`, `to_session_format`) and a test tone (`SineSource`) |
| `blastcast-mdns` | one mDNS responder shared by every protocol: our adverts (any `Service`, with `_sendspin-server._tcp` and `_mass._tcp` built in) and browsing for players |
| `blastcast-telemetry` | every timing a protocol measures: the `Series` (with its `Kind` and `Unit`), `Point` and the status types (`Stream`, `Receiver`, `Session`, `Speaker`), serde only; and behind the default `hub` feature, on tokio, the hub (`Telemetry`, `Probe`), a minute of history per series streamed live, and `Roster`, a watch of a protocol's entities |
| `blastcast-relay` | the relay above: the `Relay` that keeps every player in one group and plays the AirPlay session there, its `Store` (the server id, AirPlay 2 secret and Sendspin trust in one file), and the binary |
| `blastcast-sendspin` | the Sendspin server: handshakes, clock sync, groups, streams (Opus, FLAC, PCM) and pairing. An app hears about its players through the `Host` trait and keeps pairings with `Trust`; behind the `testing` feature, an in-memory speaker for other crates' tests |

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
depend on it, never the reverse, and the relay on them. Each protocol keeps its own clock estimator
and hands it plain data.

Its first app is
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
`crates/blastcast-sendspin`, `crates/tas-airplay-out`, and `crates/tas-airplay`
until it became `crates/blastcast-airplay`), carried over
with `git subtree split`.
