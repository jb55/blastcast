//! Reading a stream's sources on their own task, cut into 20 ms chunks.
//!
//! The stream task waits on this through a channel rather than calling
//! `next` itself. A source's read can't be safely dropped halfway (a paced
//! or resampled source loses its place), so it can't sit in a `select!`
//! beside the stream's commands. A channel receive can.
//!
//! **Play times.** A timed source's stamps survive the re-chunking: a cut
//! chunk plays at its first frame's time, counted from the latest stamp in
//! as `stamp + frames since / rate`. A stamp that agrees with that count to
//! the microsecond it was rounded to leaves the count alone, so a run of
//! consistent stamps comes out with no rounding drift at any chunk size.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use tas_audio::{Item, SESSION_FORMAT, TimedSource};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::CHUNK_FRAMES;

/// Chunks read ahead of the stream task. Small, so a source that isn't
/// live is still pulled only about as fast as it is sent.
const READ_AHEAD: usize = 2;

pub(crate) enum FeedItem {
    /// Interleaved session-format samples: [`CHUNK_FRAMES`] frames, or fewer
    /// for the last chunk before the end. `play_at_us` is when its first
    /// frame plays, if its source said; `trim_lead` is its source's
    /// [`TimedSource::trim_lead`].
    Chunk {
        samples: Vec<f32>,
        live: bool,
        play_at_us: Option<i64>,
        trim_lead: Option<i64>,
    },
    /// The source dropped what it had sent that hasn't played. What the
    /// feed held back of it is gone too.
    Flush,
    /// Every source has run out and nothing was queued behind them.
    End,
}

pub(crate) struct Feed {
    items: mpsc::Receiver<FeedItem>,
    queue: Arc<Mutex<Queue>>,
    task: JoinHandle<()>,
}

/// Sources waiting their turn. The reader sets `ended` under the same lock
/// it finds the queue empty with, so a source is either played or handed
/// back, never dropped.
#[derive(Default)]
struct Queue {
    next: VecDeque<Box<dyn TimedSource>>,
    ended: bool,
}

impl Queue {
    fn lock(queue: &Mutex<Queue>) -> std::sync::MutexGuard<'_, Queue> {
        queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Feed {
    /// Starts reading `source`, which must already be in the session format.
    pub(crate) fn spawn(source: Box<dyn TimedSource>) -> Self {
        let (tx, items) = mpsc::channel(READ_AHEAD);
        let queue = Arc::new(Mutex::new(Queue::default()));
        let task = tokio::spawn(tracing::Instrument::in_current_span(read(
            source,
            queue.clone(),
            tx,
        )));
        Self { items, queue, task }
    }

    pub(crate) async fn recv(&mut self) -> FeedItem {
        self.items.recv().await.unwrap_or(FeedItem::End)
    }

    /// Plays `source` straight after the current one, with no gap: its
    /// first frame follows the current source's last in the same chunk.
    /// Hands it back if the reader has already finished.
    pub(crate) fn queue(&self, source: Box<dyn TimedSource>) -> Result<(), Box<dyn TimedSource>> {
        let mut queue = Queue::lock(&self.queue);
        if queue.ended {
            return Err(source);
        }
        queue.next.push_back(source);
        Ok(())
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Where the frames being cut fall on the source's timeline: frame
/// `anchor_frame` of the source's frames so far plays at `anchor_us`.
#[derive(Clone, Copy)]
struct Anchor {
    us: i64,
    frame: u64,
}

impl Anchor {
    /// When `frame` plays.
    fn at(self, frame: u64) -> i64 {
        let rate = i64::from(SESSION_FORMAT.sample_rate);
        let since = frame as i64 - self.frame as i64;
        self.us + (since * 1_000_000).div_euclid(rate)
    }
}

async fn read(
    mut source: Box<dyn TimedSource>,
    queue: Arc<Mutex<Queue>>,
    tx: mpsc::Sender<FeedItem>,
) {
    let channels = usize::from(SESSION_FORMAT.channels);
    let chunk_samples = CHUNK_FRAMES * channels;
    let mut pending: Vec<f32> = Vec::with_capacity(chunk_samples * 2);
    // Frames read so far, and where the first of `pending` is among them.
    let mut frames: u64 = 0;
    let mut anchor: Option<Anchor> = None;
    let send = |samples: Vec<f32>, live, trim_lead, anchor: Option<Anchor>, first: u64| {
        let play_at_us = anchor.map(|a| a.at(first));
        tx.send(FeedItem::Chunk {
            samples,
            live,
            play_at_us,
            trim_lead,
        })
    };
    loop {
        let (live, trim_lead) = (source.is_live(), source.trim_lead());
        match source.next().await {
            Ok(Some(Item::Chunk { pcm, play_at_us })) => {
                anchor = match (play_at_us, anchor) {
                    (None, _) => None,
                    (Some(us), Some(a)) if (a.at(frames) - us).abs() <= 1 => Some(a),
                    (Some(us), _) => Some(Anchor { us, frame: frames }),
                };
                frames += pcm.frames() as u64;
                pending.extend_from_slice(&pcm.samples);
                while pending.len() >= chunk_samples {
                    let rest = pending.split_off(chunk_samples);
                    let samples = std::mem::replace(&mut pending, rest);
                    let first = frames - ((samples.len() + pending.len()) / channels) as u64;
                    if send(samples, live, trim_lead, anchor, first).await.is_err() {
                        return;
                    }
                }
                continue;
            }
            Ok(Some(Item::Flush)) => {
                pending.clear();
                anchor = None;
                if tx.send(FeedItem::Flush).await.is_err() {
                    return;
                }
                continue;
            }
            Ok(Some(Item::End) | None) => {}
            Err(e) => tracing::warn!(error = %e, "audio source failed, ending it"),
        }
        if let Some(next) = Queue::lock(&queue).next.pop_front() {
            source = next;
            continue;
        }
        if !pending.is_empty() {
            let samples = std::mem::take(&mut pending);
            let first = frames - (samples.len() / channels) as u64;
            if send(samples, live, trim_lead, anchor, first).await.is_err() {
                return;
            }
        }
        // A source queued while the tail was sent still plays gaplessly.
        {
            let mut queue = Queue::lock(&queue);
            match queue.next.pop_front() {
                Some(next) => {
                    source = next;
                    continue;
                }
                None => queue.ended = true,
            }
        }
        let _ = tx.send(FeedItem::End).await;
        return;
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use tas_audio::{AudioError, PcmChunk, SineSource};

    use super::*;

    fn untimed(source: SineSource) -> Box<dyn TimedSource> {
        Box::new(tas_audio::source::Untimed(source))
    }

    async fn frames(feed: &mut Feed) -> Vec<usize> {
        let mut out = Vec::new();
        while let FeedItem::Chunk { samples, .. } = feed.recv().await {
            out.push(samples.len() / 2);
        }
        out
    }

    /// Chunks of `sizes` frames in turn, stamped `t0 + frames before / rate`,
    /// with `t0` jumping by `jump` before chunk `jump_at`.
    struct Stamped {
        sizes: Vec<usize>,
        next: usize,
        frames: u64,
        t0: i64,
        jump_at: usize,
        jump: i64,
    }

    #[async_trait]
    impl TimedSource for Stamped {
        fn format(&self) -> tas_audio::AudioFormat {
            SESSION_FORMAT
        }

        async fn next(&mut self) -> Result<Option<Item>, AudioError> {
            let Some(&n) = self.sizes.get(self.next) else {
                return Ok(None);
            };
            if self.next == self.jump_at {
                self.t0 += self.jump;
            }
            self.next += 1;
            let play_at_us = self.t0 + (self.frames * 1_000_000 / 48_000) as i64;
            self.frames += n as u64;
            Ok(Some(Item::Chunk {
                pcm: PcmChunk::new(SESSION_FORMAT, vec![0.0; n * 2]),
                play_at_us: Some(play_at_us),
            }))
        }
    }

    async fn stamps(feed: &mut Feed) -> Vec<(Option<i64>, usize)> {
        let mut out = Vec::new();
        while let FeedItem::Chunk {
            samples,
            play_at_us,
            ..
        } = feed.recv().await
        {
            out.push((play_at_us, samples.len() / 2));
        }
        out
    }

    #[tokio::test]
    async fn cuts_any_chunk_size_into_20_ms() {
        let sine = SineSource::new(SESSION_FORMAT, 440.0)
            .chunk_frames(352)
            .limit_frames(2500);
        let mut feed = Feed::spawn(untimed(sine));
        assert_eq!(frames(&mut feed).await, [960, 960, 580]);
    }

    #[tokio::test]
    async fn a_queued_source_continues_in_the_same_chunk() {
        let a = SineSource::new(SESSION_FORMAT, 440.0).limit_frames(1000);
        let b = SineSource::new(SESSION_FORMAT, 880.0).limit_frames(1000);
        let mut feed = Feed::spawn(untimed(a));
        feed.queue(untimed(b)).map_err(|_| ()).unwrap();
        assert_eq!(frames(&mut feed).await, [960, 960, 80]);
        assert!(
            feed.queue(untimed(SineSource::new(SESSION_FORMAT, 1.0)))
                .is_err()
        );
    }

    #[tokio::test]
    async fn every_cut_keeps_its_exact_play_time_at_any_source_size() {
        for size in [1, 7, 352, 441, 959, 960, 1024, 4096] {
            let source = Stamped {
                sizes: vec![size; 48_000 / size + 1],
                next: 0,
                frames: 0,
                t0: 3_000_000,
                jump_at: usize::MAX,
                jump: 0,
            };
            let mut feed = Feed::spawn(Box::new(source));
            let mut frames = 0u64;
            for (at, n) in stamps(&mut feed).await {
                let exact = 3_000_000 + (frames * 1_000_000 / 48_000) as i64;
                assert_eq!(at, Some(exact), "size {size}, frame {frames}");
                frames += n as u64;
            }
        }
    }

    #[tokio::test]
    async fn a_jump_in_stamps_moves_the_chunks_after_it() {
        let source = Stamped {
            sizes: vec![480; 8],
            next: 0,
            frames: 0,
            t0: 0,
            jump_at: 4,
            jump: 500_000,
        };
        let mut feed = Feed::spawn(Box::new(source));
        // The jump lands on a cut, so the chunk it starts carries it whole.
        let at: Vec<_> = stamps(&mut feed)
            .await
            .into_iter()
            .map(|(at, _)| at.unwrap())
            .collect();
        assert_eq!(at, [0, 20_000, 540_000, 560_000]);
    }

    #[tokio::test]
    async fn a_flush_drops_what_was_held_back() {
        struct Flushing(u8);
        #[async_trait]
        impl TimedSource for Flushing {
            fn format(&self) -> tas_audio::AudioFormat {
                SESSION_FORMAT
            }
            async fn next(&mut self) -> Result<Option<Item>, AudioError> {
                self.0 += 1;
                Ok(Some(match self.0 {
                    1 | 3 => Item::Chunk {
                        pcm: PcmChunk::new(SESSION_FORMAT, vec![0.0; 500 * 2]),
                        play_at_us: Some(i64::from(self.0) * 1_000_000),
                    },
                    2 => Item::Flush,
                    _ => Item::End,
                }))
            }
        }
        let mut feed = Feed::spawn(Box::new(Flushing(0)));
        assert!(matches!(feed.recv().await, FeedItem::Flush));
        let FeedItem::Chunk {
            samples,
            play_at_us,
            ..
        } = feed.recv().await
        else {
            panic!("expected the chunk after the flush");
        };
        assert_eq!((samples.len() / 2, play_at_us), (500, Some(3_000_000)));
        assert!(matches!(feed.recv().await, FeedItem::End));
    }
}
