//! A test signal whose timing can be read back after a lossy codec: noise,
//! low-passed so Opus codes its waveform and doesn't just match its
//! spectrum. Unlike a sine it never repeats, so a cross-correlation has one
//! peak and can't mistake a skew of a whole period for none.

use async_trait::async_trait;
use blastcast::{AudioError, LiveSource, PcmChunk, SESSION_FORMAT, ServerClock, Timeline};

/// Noise in 20 ms chunks, delivered in real time on the server clock like
/// a capture device (or AirPlay) delivers audio.
pub struct NoiseSource {
    state: u64,
    /// Two one-pole low-pass stages, about 2.5 kHz.
    lp: [f32; 2],
    clock: ServerClock,
    timeline: Timeline,
}

impl NoiseSource {
    /// `seed` picks the noise, so two sources can't be confused.
    pub fn new(seed: u64, clock: ServerClock) -> Self {
        Self {
            state: seed.max(1),
            lp: [0.0; 2],
            timeline: Timeline::new(clock.now_us(), SESSION_FORMAT.sample_rate),
            clock,
        }
    }

    fn white(&mut self) -> f32 {
        // xorshift64
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        (self.state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }
}

#[async_trait]
impl LiveSource for NoiseSource {
    fn format(&self) -> blastcast::AudioFormat {
        SESSION_FORMAT
    }

    fn is_live(&self) -> bool {
        true
    }

    async fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError> {
        const FRAMES: usize = SESSION_FORMAT.sample_rate as usize / 50;
        const ALPHA: f32 = 0.3;
        let span = self.timeline.advance(FRAMES as u64);
        tokio::time::sleep_until(self.clock.instant_at(span.end_us())).await;
        let mut samples = Vec::with_capacity(FRAMES * 2);
        for _ in 0..FRAMES {
            let x = self.white();
            self.lp[0] += ALPHA * (x - self.lp[0]);
            self.lp[1] += ALPHA * (self.lp[0] - self.lp[1]);
            let s = (self.lp[1] * 1.5).clamp(-0.9, 0.9);
            samples.extend([s, s]);
        }
        Ok(Some(PcmChunk::new(SESSION_FORMAT, samples)))
    }
}
