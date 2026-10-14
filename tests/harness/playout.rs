//! What a player played, and when: decoded audio laid out on the *true*
//! server clock, the one the test can read and the player can only
//! estimate. A player puts each chunk at its timestamp through its time
//! filter, so the filter's error is in the record, as it would be in the
//! room.
//!
//! The record follows the spec's player: a chunk that lands on audio
//! already held replaces it from its own start (an in-place `stream/start`
//! keeps the buffer, and the new format's chunks take over), and
//! `stream/clear` or `stream/end` drops everything not yet played.

/// The test's one rate: every format the harness asks for is 48 kHz.
pub const RATE: u32 = 48_000;

/// Holes up to this wide are the time filter moving a few µs between two
/// chunks, which a player's sync correction absorbs, not missing audio.
pub const MAX_HOLE_US: f64 = 100.0;

/// How far apart two players' audio is looked for: ±10 ms, past an
/// uncompensated Opus lookahead (6.5 ms).
pub const MAX_LAG_FRAMES: usize = 480;

#[derive(Debug, Clone)]
struct Run {
    /// When the first sample plays, true server clock, µs.
    start_us: f64,
    samples: Vec<f32>,
}

impl Run {
    fn end_us(&self) -> f64 {
        self.start_us + frames_to_us(self.samples.len())
    }

    /// Drops the samples that play at or after `at_us`.
    fn truncate(&mut self, at_us: f64) {
        let keep = ((at_us - self.start_us) * f64::from(RATE) / 1e6).ceil();
        self.samples.truncate(keep.max(0.0) as usize);
    }
}

fn frames_to_us(frames: usize) -> f64 {
    frames as f64 * 1e6 / f64::from(RATE)
}

/// One player's output, mono, in play order.
#[derive(Debug, Clone, Default)]
pub struct Playout {
    runs: Vec<Run>,
}

impl Playout {
    /// A chunk that plays from `start_us`.
    pub fn insert(&mut self, start_us: f64, samples: Vec<f32>) {
        self.cut(start_us);
        self.runs.push(Run { start_us, samples });
    }

    /// Drops everything that would play at or after `at_us`.
    pub fn cut(&mut self, at_us: f64) {
        for run in &mut self.runs {
            if run.end_us() > at_us {
                run.truncate(at_us);
            }
        }
        self.runs.retain(|r| !r.samples.is_empty());
    }

    /// When the last sample ends, if anything played.
    pub fn end_us(&self) -> Option<f64> {
        self.runs.last().map(Run::end_us)
    }

    /// The silences wider than [`MAX_HOLE_US`] between `from_us` and `to_us`.
    pub fn holes(&self, from_us: f64, to_us: f64) -> Vec<(f64, f64)> {
        let mut holes = Vec::new();
        let mut cursor = from_us;
        for run in self.runs.iter().filter(|r| r.end_us() > from_us) {
            if run.start_us >= to_us {
                break;
            }
            if run.start_us > cursor + MAX_HOLE_US {
                holes.push((cursor, run.start_us));
            }
            cursor = cursor.max(run.end_us());
        }
        if to_us > cursor + MAX_HOLE_US {
            holes.push((cursor, to_us));
        }
        holes
    }

    /// `frames` samples at [`RATE`] from `from_us`: what was playing at
    /// each instant, or an error at the first one where nothing was.
    pub fn render(&self, from_us: f64, frames: usize) -> Result<Vec<f32>, String> {
        let mut out = Vec::with_capacity(frames);
        let mut i = self
            .runs
            .partition_point(|r| r.start_us <= from_us)
            .saturating_sub(1);
        for n in 0..frames {
            let t = from_us + frames_to_us(n);
            while i + 1 < self.runs.len() && self.runs[i + 1].start_us <= t + MAX_HOLE_US {
                i += 1;
            }
            let run = self
                .runs
                .get(i)
                .filter(|r| r.start_us <= t + MAX_HOLE_US && t < r.end_us() + MAX_HOLE_US)
                .ok_or_else(|| format!("nothing playing at {t:.0} µs"))?;
            let k = ((t - run.start_us) * f64::from(RATE) / 1e6).round();
            out.push(run.samples[(k.max(0.0) as usize).min(run.samples.len() - 1)]);
        }
        Ok(out)
    }
}

/// How two players' audio lines up over one window.
#[derive(Debug, Clone, Copy)]
pub struct Skew {
    /// How much later `b` plays the same audio than `a`, µs.
    pub lag_us: f64,
    /// The normalized cross-correlation at that lag: 1.0 is the same
    /// waveform, lossless; Opus lands a little under.
    pub correlation: f64,
}

/// Cross-correlates `b` against `a` over `frames` from `from_us`, at lags
/// up to [`MAX_LAG_FRAMES`] either way, and refines the peak to a
/// fraction of a sample.
pub fn skew(a: &Playout, b: &Playout, from_us: f64, frames: usize) -> Result<Skew, String> {
    let lag = MAX_LAG_FRAMES;
    let ra = a.render(from_us, frames)?;
    let rb = b.render(from_us - frames_to_us(lag), frames + 2 * lag)?;
    let energy = |s: &[f32]| s.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>();
    let ea = energy(&ra);
    let corr: Vec<f64> = (0..=2 * lag)
        .map(|k| {
            let w = &rb[k..k + frames];
            let dot: f64 = ra
                .iter()
                .zip(w)
                .map(|(x, y)| f64::from(*x) * f64::from(*y))
                .sum();
            dot / (ea * energy(w)).sqrt().max(f64::MIN_POSITIVE)
        })
        .collect();
    let (best, &peak) = corr
        .iter()
        .enumerate()
        .max_by(|x, y| x.1.total_cmp(y.1))
        .expect("at least one lag");
    let fraction = if best > 0 && best < 2 * lag {
        let (y0, y2) = (corr[best - 1], corr[best + 1]);
        let denom = y0 - 2.0 * peak + y2;
        if denom.abs() > f64::EPSILON {
            0.5 * (y0 - y2) / denom
        } else {
            0.0
        }
    } else {
        0.0
    };
    Ok(Skew {
        lag_us: (best as f64 - lag as f64 + fraction) * 1e6 / f64::from(RATE),
        correlation: peak,
    })
}
