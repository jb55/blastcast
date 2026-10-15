//! A streaming sample-rate converter over rubato, for live input that
//! arrives in chunks of any size.
//!
//! rubato works on fixed-size blocks, so input is buffered until a whole
//! block is in. Its filter delay (a few hundred frames of leading silence)
//! is trimmed from the output, so output frame `n` lines up with input time
//! `n / to_rate`: a source's timeline doesn't shift when it is resampled.
//! [`StreamResampler::flush`] pads the tail out so the total output is
//! exactly `round(frames_in * to / from)` frames.
//!
//! It uses rubato's asynchronous sinc resampler rather than the cheaper
//! fixed-ratio FFT one, because only the asynchronous one can nudge its ratio
//! later to follow a sender's clock drift.

use rubato::audioadapter_buffers::direct::InterleavedSlice;
use rubato::{
    Async, FixedAsync, Indexing, Resampler, SincInterpolationParameters, SincInterpolationType,
    WindowFunction,
};

use crate::AudioError;

/// Input frames per rubato block: about 23 ms at 44.1 kHz.
const BLOCK_FRAMES: usize = 1024;
/// How far the ratio may later be adjusted from nominal, for drift.
const MAX_RELATIVE_RATIO: f64 = 1.01;

pub struct StreamResampler {
    from: u32,
    to: u32,
    channels: usize,
    /// `None` when the rates match and samples pass straight through.
    inner: Option<Async<f32>>,
    /// Interleaved input not yet resampled.
    pending: Vec<f32>,
    /// rubato's output block.
    block_out: Vec<f32>,
    /// Leading output frames still to drop: the filter delay.
    skip_frames: usize,
    frames_in: u64,
    frames_out: u64,
}

impl StreamResampler {
    pub fn new(from: u32, to: u32, channels: u16) -> Result<Self, AudioError> {
        if from == 0 || to == 0 || channels == 0 {
            return Err(AudioError::Format(format!(
                "cannot resample {from} Hz to {to} Hz with {channels} channels"
            )));
        }
        let channels = usize::from(channels);
        let inner = if from == to {
            None
        } else {
            let params = SincInterpolationParameters {
                sinc_len: 128,
                f_cutoff: None,
                oversampling_factor: 128,
                interpolation: SincInterpolationType::Cubic,
                window: WindowFunction::BlackmanHarris2,
            };
            let resampler = Async::<f32>::new_sinc(
                f64::from(to) / f64::from(from),
                MAX_RELATIVE_RATIO,
                &params,
                BLOCK_FRAMES,
                channels,
                FixedAsync::Input,
            )
            .map_err(|e| AudioError::Resampler(e.to_string()))?;
            Some(resampler)
        };
        let block_out = vec![0.0; inner.as_ref().map_or(0, |r| r.output_frames_max()) * channels];
        let skip_frames = inner.as_ref().map_or(0, |r| r.output_delay());
        Ok(Self {
            from,
            to,
            channels,
            inner,
            pending: Vec::new(),
            block_out,
            skip_frames,
            frames_in: 0,
            frames_out: 0,
        })
    }

    /// Resample interleaved `input`, appending whatever output is ready to
    /// `out`. Up to a block of input may be held back until more arrives.
    pub fn process(&mut self, input: &[f32], out: &mut Vec<f32>) -> Result<(), AudioError> {
        if !input.len().is_multiple_of(self.channels) {
            return Err(AudioError::Format(format!(
                "{} samples is not a whole number of {}-channel frames",
                input.len(),
                self.channels
            )));
        }
        self.frames_in += (input.len() / self.channels) as u64;
        if self.inner.is_none() {
            out.extend_from_slice(input);
            self.frames_out = self.frames_in;
            return Ok(());
        }
        self.pending.extend_from_slice(input);
        let mut offset = 0;
        loop {
            let available = self.pending.len() / self.channels - offset;
            if available < self.block_frames_next() {
                break;
            }
            let indexing = Indexing::new().input_offset(offset);
            offset += self.run_block(&indexing, out)?;
        }
        self.pending.drain(..offset * self.channels);
        Ok(())
    }

    /// Resample whatever input is held back and drain the filter, so the
    /// output totals exactly `round(frames_in * to / from)` frames. The
    /// resampler is then ready for an unrelated stream.
    pub fn flush(&mut self, out: &mut Vec<f32>) -> Result<(), AudioError> {
        if self.inner.is_some() {
            let expected = (self.frames_in * u64::from(self.to) + u64::from(self.from) / 2)
                / u64::from(self.from);
            let start = out.len();
            while self.frames_out < expected {
                let left = self.pending.len() / self.channels;
                let indexing = Indexing::new().partial_len(left);
                self.run_block(&indexing, out)?;
                self.pending.clear();
            }
            let excess = (self.frames_out - expected) as usize * self.channels;
            out.truncate(out.len() - excess.min(out.len() - start));
        }
        self.reset();
        Ok(())
    }

    /// Drop held-back input and filter state, e.g. after a gap in the stream.
    pub fn reset(&mut self) {
        if let Some(inner) = &mut self.inner {
            inner.reset();
            self.skip_frames = inner.output_delay();
        }
        self.pending.clear();
        self.frames_in = 0;
        self.frames_out = 0;
    }

    fn block_frames_next(&self) -> usize {
        self.inner.as_ref().map_or(0, |r| r.input_frames_next())
    }

    /// Run one rubato block from `pending`, append its output past the
    /// filter delay, and return the input frames it consumed.
    fn run_block(&mut self, indexing: &Indexing, out: &mut Vec<f32>) -> Result<usize, AudioError> {
        let Some(inner) = &mut self.inner else {
            return Ok(0);
        };
        let ch = self.channels;
        let input = InterleavedSlice::new(&self.pending[..], ch, self.pending.len() / ch)
            .map_err(|e| AudioError::Resampler(e.to_string()))?;
        let out_frames = self.block_out.len() / ch;
        let mut output = InterleavedSlice::new_mut(&mut self.block_out[..], ch, out_frames)
            .map_err(|e| AudioError::Resampler(e.to_string()))?;
        let (consumed, produced) = inner
            .process_into_buffer(&input, &mut output, Some(indexing))
            .map_err(|e| AudioError::Resampler(e.to_string()))?;
        let skip = self.skip_frames.min(produced);
        self.skip_frames -= skip;
        out.extend_from_slice(&self.block_out[skip * ch..produced * ch]);
        self.frames_out += (produced - skip) as u64;
        Ok(consumed)
    }
}

#[cfg(test)]
mod tests {
    use std::f64::consts::TAU;

    use super::*;

    /// `frames` of a stereo sine at `rate`, left and right equal.
    fn sine(freq: f64, rate: u32, frames: usize) -> Vec<f32> {
        (0..frames)
            .flat_map(|n| {
                let s = (TAU * freq * n as f64 / f64::from(rate)).sin() as f32 * 0.5;
                [s, s]
            })
            .collect()
    }

    fn resample_all(input: &[f32], chunk_frames: usize) -> Vec<f32> {
        let mut r = StreamResampler::new(44_100, 48_000, 2).unwrap();
        let mut out = Vec::new();
        for chunk in input.chunks(chunk_frames * 2) {
            r.process(chunk, &mut out).unwrap();
        }
        r.flush(&mut out).unwrap();
        out
    }

    #[test]
    fn one_second_at_44k1_is_one_second_at_48k_and_in_phase() {
        let out = resample_all(&sine(1000.0, 44_100, 44_100), 352);
        assert_eq!(out.len(), 48_000 * 2);
        // With the filter delay trimmed, the output is the same sine sampled
        // at 48 kHz, shifted by less than a frame: rubato reports its delay
        // in whole frames, and the fraction left over is the same for every
        // speaker. Find the shift that fits best, skipping the edges, where
        // the filter sees silence past either end.
        let error_at = |shift_frames: f64| {
            (1000..47_000)
                .map(|n| {
                    let t = (n as f64 - shift_frames) / 48_000.0;
                    let ideal = (TAU * 1000.0 * t).sin() as f32 * 0.5;
                    (out[n * 2] - ideal).abs()
                })
                .fold(0.0, f32::max)
        };
        let (shift, worst) = (-100..=100)
            .map(|i| f64::from(i) / 100.0)
            .map(|shift| (shift, error_at(shift)))
            .min_by(|a, b| a.1.total_cmp(&b.1))
            .unwrap();
        assert!(
            worst < 1e-3,
            "max error {worst} at a shift of {shift} frames"
        );
        assert!(shift.abs() < 0.5, "output is shifted by {shift} frames");
    }

    #[test]
    fn chunking_does_not_change_the_output() {
        let input = sine(440.0, 44_100, 20_000);
        let a = resample_all(&input, 1);
        let b = resample_all(&input, 4096);
        let c = resample_all(&input, 20_000);
        assert_eq!(a.len(), (20_000 * 48_000 + 22_050) / 44_100 * 2);
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn short_input_still_comes_out_whole() {
        // Less than one block in, and less than the filter delay out.
        let out = resample_all(&sine(440.0, 44_100, 100), 100);
        assert_eq!(out.len(), (100 * 48_000 + 22_050) / 44_100 * 2);
    }

    #[test]
    fn reusable_after_flush() {
        let input = sine(440.0, 44_100, 5000);
        let mut r = StreamResampler::new(44_100, 48_000, 2).unwrap();
        let (mut first, mut second) = (Vec::new(), Vec::new());
        r.process(&input, &mut first).unwrap();
        r.flush(&mut first).unwrap();
        r.process(&input, &mut second).unwrap();
        r.flush(&mut second).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn equal_rates_pass_through() {
        let input = sine(440.0, 48_000, 1000);
        let mut r = StreamResampler::new(48_000, 48_000, 2).unwrap();
        let mut out = Vec::new();
        r.process(&input, &mut out).unwrap();
        r.flush(&mut out).unwrap();
        assert_eq!(out, input);
    }

    #[test]
    fn rejects_partial_frames() {
        let mut r = StreamResampler::new(44_100, 48_000, 2).unwrap();
        assert!(r.process(&[0.0; 3], &mut Vec::new()).is_err());
    }
}
