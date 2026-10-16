//! Sources of audio: a test tone, a resampled source, and the conversion
//! to the session format. The [`LiveSource`] and [`TimedSource`] traits
//! themselves are blastcast's.

use std::f64::consts::TAU;

use async_trait::async_trait;

use blastcast::{
    AudioError, AudioFormat, Item, LiveSource, PcmChunk, SESSION_FORMAT, ServerClock, TimedSource,
    Timeline,
};

use crate::resample::StreamResampler;

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

    fn source_format(&self) -> AudioFormat {
        self.inner.source_format()
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

/// A [`TimedSource`] converted to another sample rate, its play times
/// carried through. Each stamped chunk in places the frames already fed
/// since the last reset on the timeline, and an output frame `n` plays at
/// `n / to_rate` past the first's, since the resampler trims its filter
/// delay. A flush resets the resampler.
pub struct TimedResampled<S> {
    inner: S,
    format: AudioFormat,
    resampler: StreamResampler,
    /// When the first frame in since the last reset plays, as the latest
    /// stamped chunk puts it.
    origin_us: Option<i64>,
    frames_in: u64,
    frames_out: u64,
    /// An item to yield after the chunk the resampler's tail made.
    after: Option<Item>,
}

impl<S: TimedSource> TimedResampled<S> {
    pub fn new(inner: S, sample_rate: u32) -> Result<Self, AudioError> {
        let from = inner.format();
        let resampler = StreamResampler::new(from.sample_rate, sample_rate, from.channels)?;
        Ok(Self {
            inner,
            format: AudioFormat::new(sample_rate, from.channels),
            resampler,
            origin_us: None,
            frames_in: 0,
            frames_out: 0,
            after: None,
        })
    }

    fn reset(&mut self) {
        self.resampler.reset();
        self.origin_us = None;
        self.frames_in = 0;
        self.frames_out = 0;
    }

    /// `out`, which came after `frames_out` frames, as a chunk.
    fn chunk(&mut self, out: Vec<f32>) -> Item {
        let rate = i64::from(self.format.sample_rate);
        let play_at_us = self
            .origin_us
            .map(|origin| origin + self.frames_out as i64 * 1_000_000 / rate);
        self.frames_out += (out.len() / usize::from(self.format.channels)) as u64;
        Item::Chunk {
            pcm: PcmChunk::new(self.format, out),
            play_at_us,
        }
    }
}

#[async_trait]
impl<S: TimedSource> TimedSource for TimedResampled<S> {
    fn format(&self) -> AudioFormat {
        self.format
    }

    fn source_format(&self) -> AudioFormat {
        self.inner.source_format()
    }

    fn is_live(&self) -> bool {
        self.inner.is_live()
    }

    fn kind(&self) -> &'static str {
        self.inner.kind()
    }

    fn trim_lead(&self) -> Option<i64> {
        self.inner.trim_lead()
    }

    fn on_time(&self) -> bool {
        self.inner.on_time()
    }

    async fn next(&mut self) -> Result<Option<Item>, AudioError> {
        if let Some(item) = self.after.take() {
            return Ok(Some(item));
        }
        let mut out = Vec::new();
        // The resampler holds input back until it has a whole block, so a
        // small input chunk may yield nothing: keep pulling until it does.
        loop {
            match self.inner.next().await? {
                Some(Item::Chunk { pcm, play_at_us }) => {
                    let expected = self.inner.format();
                    if pcm.format != expected {
                        return Err(AudioError::Format(format!(
                            "source said {expected} but sent {}",
                            pcm.format
                        )));
                    }
                    if let Some(at) = play_at_us {
                        let rate = i64::from(expected.sample_rate);
                        self.origin_us = Some(at - self.frames_in as i64 * 1_000_000 / rate);
                    }
                    self.frames_in += pcm.frames() as u64;
                    self.resampler.process(&pcm.samples, &mut out)?;
                    if !out.is_empty() {
                        return Ok(Some(self.chunk(out)));
                    }
                }
                // What is held back hasn't played, so it goes with the rest.
                Some(Item::Flush) => {
                    self.reset();
                    return Ok(Some(Item::Flush));
                }
                Some(Item::End) | None => {
                    self.resampler.flush(&mut out)?;
                    if out.is_empty() {
                        return Ok(Some(Item::End));
                    }
                    let chunk = self.chunk(out);
                    self.reset();
                    self.after = Some(Item::End);
                    return Ok(Some(chunk));
                }
            }
        }
    }
}

/// A timed `source` in the session format, resampled if it needs to be.
/// As [`to_session_format`], with each chunk's play time kept.
pub fn timed_to_session_format(
    source: Box<dyn TimedSource>,
) -> Result<Box<dyn TimedSource>, AudioError> {
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
    Ok(Box::new(TimedResampled::new(
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
        assert_eq!(session.source_format(), CD, "what it was given");
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

    /// A 44.1 kHz source whose chunks of `chunk_frames` are stamped
    /// `t0 + frames before * 1e6 / 44_100`, with a flush after `flush_at`
    /// chunks, after which the stamps start again at `t1`.
    struct Stamped {
        chunks: usize,
        sent: usize,
        frames: u64,
        flush_at: Option<usize>,
        t0: i64,
    }

    #[async_trait]
    impl TimedSource for Stamped {
        fn format(&self) -> AudioFormat {
            CD
        }

        async fn next(&mut self) -> Result<Option<Item>, AudioError> {
            if self.flush_at == Some(self.sent) {
                self.flush_at = None;
                self.frames = 0;
                self.t0 = 9_000_000;
                return Ok(Some(Item::Flush));
            }
            if self.sent == self.chunks {
                return Ok(None);
            }
            self.sent += 1;
            let play_at_us = self.t0 + (self.frames * 1_000_000 / 44_100) as i64;
            self.frames += 352;
            Ok(Some(Item::Chunk {
                pcm: PcmChunk::new(CD, vec![0.0; 352 * 2]),
                play_at_us: Some(play_at_us),
            }))
        }
    }

    /// Every item, chunks as (play time, frames).
    async fn items(
        source: &mut dyn TimedSource,
    ) -> Vec<Result<(Option<i64>, usize), &'static str>> {
        let mut out = Vec::new();
        loop {
            match source.next().await.unwrap() {
                Some(Item::Chunk { pcm, play_at_us }) => out.push(Ok((play_at_us, pcm.frames()))),
                Some(Item::Flush) => out.push(Err("flush")),
                Some(Item::End) | None => return out,
            }
        }
    }

    #[tokio::test]
    async fn a_timed_source_keeps_its_play_times_through_resampling() {
        let stamped = Stamped {
            chunks: 250,
            sent: 0,
            frames: 0,
            flush_at: None,
            t0: 1_000_000,
        };
        let mut session = timed_to_session_format(Box::new(stamped)).unwrap();
        assert_eq!(session.format(), SESSION_FORMAT);
        assert_eq!(session.source_format(), CD);
        let mut frames = 0u64;
        for item in items(&mut *session).await {
            let (at, n) = item.unwrap();
            // To within the microsecond each stamp was rounded to.
            let exact = 1_000_000 + (frames * 1_000_000 / 48_000) as i64;
            assert!((at.unwrap() - exact).abs() <= 1, "{at:?} vs {exact}");
            frames += n as u64;
        }
        assert_eq!(frames, (250 * 352 * 48_000 + 22_050) / 44_100);
    }

    #[tokio::test]
    async fn a_flush_drops_what_the_resampler_held_and_places_afresh() {
        let stamped = Stamped {
            chunks: 40,
            sent: 0,
            frames: 0,
            flush_at: Some(20),
            t0: 1_000_000,
        };
        let mut session = timed_to_session_format(Box::new(stamped)).unwrap();
        let items = items(&mut *session).await;
        let flush = items.iter().position(|i| i.is_err()).unwrap();
        let (at, _) = items[flush + 1].unwrap();
        assert_eq!(at, Some(9_000_000), "the first chunk after it");
        let before: usize = items[..flush].iter().map(|i| i.unwrap().1).sum();
        assert!(before < 20 * 352 * 48_000 / 44_100, "the tail was dropped");
    }

    #[tokio::test]
    async fn an_untimed_source_stays_untimed() {
        let sine = SineSource::new(CD, 1000.0).limit_frames(4410);
        let mut session = timed_to_session_format(Box::new(blastcast::Untimed(sine))).unwrap();
        let items = items(&mut *session).await;
        assert!(items.iter().all(|i| i.unwrap().0.is_none()));
        assert_eq!(items.iter().map(|i| i.unwrap().1).sum::<usize>(), 4800);
    }
}
