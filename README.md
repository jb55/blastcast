# blastcast

One timeline across every audio protocol.

Every protocol has its own clock (RAOP's NTP, AirPlay 2's PTP, Sendspin's
time sync) and its own way of saying "this frame plays then". blastcast puts
them all on one clock, the `ServerClock`. Each input maps its sender's clock
to it with a `ClockMap` and says when its frames play with an `Anchor`, so
any input can feed any output.

| module | what it holds |
|---|---|
| `clock` | `ServerClock`: microseconds since start, on tokio's (pausable) `Instant` |
| `format` | `AudioFormat`, `PcmChunk` and the session format |
| `map` | `ClockMap` (offset, drift, error against the server clock) and `Anchor` (frame F plays at remote time T, at rate R) |
| `source` | `LiveSource`, and `TimedSource` yielding `Item::{Chunk, Flush, End}` |
| `rate` | `RateMatch`: absorbs the drift between a sender's clock and ours, measured against stamps |
| `timeline` | `Timeline` and `Span`: the server-clock time of each chunk from the frames before it, exact over any run |

It knows no protocol and no server. Protocol code depends on blastcast, never
the reverse. Each protocol keeps its own clock estimator and hands blastcast
plain data.

Its first user is
[tune-assistant-server](https://github.com/jb55/tune-assistant-server),
whose RAOP and AirPlay 2 receivers are timed sources and whose Sendspin
stream is the sink. It depends on this repo by path, as a sibling checkout
(`../blastcast`).

## Checking

```sh
./scripts/check.sh   # fmt, clippy with warnings denied, tests
```

The history before this repo existed is that of `crates/blastcast` in
tune-assistant-server, carried over with `git subtree split`.
