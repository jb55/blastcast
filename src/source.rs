//! Sources of audio: a test tone, a resampled source, and the conversion
//! to the session format. The [`LiveSource`] trait itself is blastcast's.

use std::f64::consts::TAU;

use async_trait::async_trait;

use crate::AudioError;
use crate::clock::ServerClock;
use crate::format::{AudioFormat, PcmChunk, SESSION_FORMAT};
use crate::resample::StreamResampler;
use crate::timeline::Timeline;

pub use blastcast::LiveSource;

/// A test tone, for exercising Sendspin output before AirPlay input exists.
///
/// Unpaced, it yields chunks as fast as it is pulled. [`SineSource::paced`]
/// makes it live: each chunk is held until the server clock has reached the
/// end of it, the way a capture device delivers audio.
pub struct SineSource {
    format: AudioFormat,
    freq_hz: f64,
    amplitude: f32,
    chunk_frames: usize,
    /// Frames still to produce, or `None` to run forever.
    remaining: Option<u64>,
    /// Phase of the next frame, in cycles (0.0..1.0).
    phase: f64,
    pacing: Option<(ServerClock, Timeline)>,
}

impl SineSource {
    /// A -6 dBFS sine at `freq_hz`, in 20 ms chunks.
    pub fn new(format: AudioFormat, freq_hz: f64) -> Self {
        Self {
            format,
            freq_hz,
            amplitude: 0.5,
            chunk_frames: (format.sample_rate / 50) as usize,
            remaining: None,
            phase: 0.0,
            pacing: None,
        }
    }

    pub fn amplitude(mut self, amplitude: f32) -> Self {
        self.amplitude = amplitude;
        self
    }

    pub fn chunk_frames(mut self, frames: usize) -> Self {
        assert!(frames > 0, "chunks must hold at least one frame");
        self.chunk_frames = frames;
        self
    }

    /// End the stream after `frames` frames.
    pub fn limit_frames(mut self, frames: u64) -> Self {
        self.remaining = Some(frames);
        self
    }

    /// Deliver in real time on `clock`, starting now.
    pub fn paced(mut self, clock: ServerClock) -> Self {
        let timeline = Timeline::new(clock.now_us(), self.format.sample_rate);
        self.pacing = Some((clock, timeline));
        self
    }
}

#[async_trait]
impl LiveSource for SineSource {
    fn format(&self) -> AudioFormat {
        self.format
    }

    fn is_live(&self) -> bool {
        self.pacing.is_some()
    }

    fn kind(&self) -> &'static str {
        "tone"
    }

    async fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError> {
        let frames = match self.remaining {
            Some(0) => return Ok(None),
            Some(left) => left.min(self.chunk_frames as u64) as usize,
            None => self.chunk_frames,
        };
        if let Some((clock, timeline)) = &mut self.pacing {
            let span = timeline.advance(frames as u64);
            tokio::time::sleep_until(clock.instant_at(span.end_us())).await;
        }
        let channels = usize::from(self.format.channels);
        let step = self.freq_hz / f64::from(self.format.sample_rate);
        let mut samples = Vec::with_capacity(frames * channels);
        for _ in 0..frames {
            let s = (TAU * self.phase).sin() as f32 * self.amplitude;
            samples.extend(std::iter::repeat_n(s, channels));
            self.phase = (self.phase + step).fract();
        }
        if let Some(left) = &mut self.remaining {
            *left -= frames as u64;
        }
        Ok(Some(PcmChunk::new(self.format, samples)))
    }
}

/// A source converted to another sample rate. Channel counts must match.
pub struct Resampled<S> {
    inner: S,
    format: AudioFormat,
    resampler: StreamResampler,
    ended: bool,
}

impl<S: LiveSource> Resampled<S> {
    pub fn new(inner: S, sample_rate: u32) -> Result<Self, AudioError> {
        let from = inner.format();
        let resampler = StreamResampler::new(from.sample_rate, sample_rate, from.channels)?;
        Ok(Self {
            inner,
            format: AudioFormat::new(sample_rate, from.channels),
            resampler,
            ended: false,
        })
    }
}

#[async_trait]
impl<S: LiveSource> LiveSource for Resampled<S> {
    fn format(&self) -> AudioFormat {
        self.format
    }

    fn is_live(&self) -> bool {
        self.inner.is_live()
    }

    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    async fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError> {
        let mut out = Vec::new();
        // The resampler holds input back until it has a whole block, so a
        // small input chunk may yield nothing: keep pulling until it does.
        while out.is_empty() && !self.ended {
            match self.inner.next_chunk().await? {
                Some(chunk) => {
                    let expected = self.inner.format();
                    if chunk.format != expected {
                        return Err(AudioError::Format(format!(
                            "source said {expected} but sent {}",
                            chunk.format
                        )));
                    }
                    self.resampler.process(&chunk.samples, &mut out)?;
                }
                None => {
                    self.ended = true;
                    self.resampler.flush(&mut out)?;
                }
            }
        }
        Ok((!out.is_empty()).then(|| PcmChunk::new(self.format, out)))
    }
}

/// `source` in the session format, resampled if it needs to be.
pub fn to_session_format(source: Box<dyn LiveSource>) -> Result<Box<dyn LiveSource>, AudioError> {
    let format = source.format();
    if format.channels != SESSION_FORMAT.channels {
        return Err(AudioError::Format(format!(
            "{format} source: only {}-channel sources are supported",
            SESSION_FORMAT.channels
        )));
    }
    if format.sample_rate == SESSION_FORMAT.sample_rate {
        return Ok(source);
    }
    Ok(Box::new(Resampled::new(
        source,
        SESSION_FORMAT.sample_rate,
    )?))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const CD: AudioFormat = AudioFormat::new(44_100, 2);

    async fn drain(source: &mut dyn LiveSource) -> Vec<PcmChunk> {
        let mut chunks = Vec::new();
        while let Some(chunk) = source.next_chunk().await.unwrap() {
            assert!(!chunk.is_empty());
            assert_eq!(chunk.format, source.format());
            chunks.push(chunk);
        }
        chunks
    }

    #[tokio::test]
    async fn unpaced_sine_yields_exactly_its_limit() {
        let mut sine = SineSource::new(CD, 1000.0)
            .chunk_frames(1000)
            .limit_frames(2500);
        assert!(!sine.is_live());
        let frames: Vec<usize> = drain(&mut sine)
            .await
            .iter()
            .map(PcmChunk::frames)
            .collect();
        assert_eq!(frames, [1000, 1000, 500]);
    }

    #[tokio::test]
    async fn sine_is_continuous_across_chunks() {
        let mut chunked = SineSource::new(CD, 997.0).chunk_frames(7).limit_frames(700);
        let mut whole = SineSource::new(CD, 997.0)
            .chunk_frames(700)
            .limit_frames(700);
        let a: Vec<f32> = drain(&mut chunked)
            .await
            .into_iter()
            .flat_map(|c| c.samples)
            .collect();
        let b = whole.next_chunk().await.unwrap().unwrap().samples;
        assert_eq!(a, b);
        assert_eq!(b[0], 0.0);
        assert!(b.iter().all(|s| s.abs() <= 0.5));
    }

    #[tokio::test(start_paused = true)]
    async fn paced_sine_delivers_in_real_time() {
        let clock = ServerClock::new();
        let mut sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
        assert!(sine.is_live());
        for n in 1..=50 {
            sine.next_chunk().await.unwrap().unwrap();
            // Each 20 ms chunk is ready when its last frame has "happened".
            assert_eq!(clock.now_us(), n * 20_000);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn paced_sine_waits_for_the_clock() {
        let clock = ServerClock::new();
        let mut sine = SineSource::new(SESSION_FORMAT, 440.0).paced(clock);
        let pull = sine.next_chunk();
        tokio::pin!(pull);
        assert!(
            tokio::time::timeout(Duration::from_millis(19), &mut pull)
                .await
                .is_err()
        );
        assert!(pull.await.unwrap().is_some());
    }

    #[tokio::test]
    async fn resampled_source_comes_out_in_the_session_format() {
        let sine = SineSource::new(CD, 1000.0)
            .chunk_frames(352)
            .limit_frames(44_100);
        let mut session = to_session_format(Box::new(sine)).unwrap();
        assert_eq!(session.format(), SESSION_FORMAT);
        assert!(!session.is_live());
        let frames: usize = drain(&mut *session)
            .await
            .iter()
            .map(PcmChunk::frames)
            .sum();
        assert_eq!(frames, 48_000);
    }

    #[tokio::test]
    async fn session_rate_sources_are_not_wrapped() {
        let sine = SineSource::new(SESSION_FORMAT, 1000.0).limit_frames(960);
        let mut session = to_session_format(Box::new(sine)).unwrap();
        assert_eq!(drain(&mut *session).await.len(), 1);
    }

    #[test]
    fn mono_sources_are_refused() {
        let sine = SineSource::new(AudioFormat::new(48_000, 1), 1000.0);
        assert!(to_session_format(Box::new(sine)).is_err());
    }
}
