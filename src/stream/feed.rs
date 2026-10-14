//! Reading a stream's sources on their own task, cut into 20 ms chunks.
//!
//! The stream task waits on this through a channel rather than calling
//! `next_chunk` itself. A source's read can't be safely dropped halfway
//! (a paced or resampled source loses its place), so it can't sit in a
//! `select!` beside the stream's commands. A channel receive can.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

use tas_audio::{LiveSource, SESSION_FORMAT};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::CHUNK_FRAMES;

/// Chunks read ahead of the stream task. Small, so a source that isn't
/// live is still pulled only about as fast as it is sent.
const READ_AHEAD: usize = 2;

pub(crate) enum FeedItem {
    /// Interleaved session-format samples: [`CHUNK_FRAMES`] frames, or fewer
    /// for the last chunk before the end.
    Chunk { samples: Vec<f32>, live: bool },
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
    next: VecDeque<Box<dyn LiveSource>>,
    ended: bool,
}

impl Queue {
    fn lock(queue: &Mutex<Queue>) -> std::sync::MutexGuard<'_, Queue> {
        queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Feed {
    /// Starts reading `source`, which must already be in the session format.
    pub(crate) fn spawn(source: Box<dyn LiveSource>) -> Self {
        let (tx, items) = mpsc::channel(READ_AHEAD);
        let queue = Arc::new(Mutex::new(Queue::default()));
        let task = tokio::spawn(read(source, queue.clone(), tx));
        Self { items, queue, task }
    }

    pub(crate) async fn recv(&mut self) -> FeedItem {
        self.items.recv().await.unwrap_or(FeedItem::End)
    }

    /// Plays `source` straight after the current one, with no gap: its
    /// first frame follows the current source's last in the same chunk.
    /// Hands it back if the reader has already finished.
    pub(crate) fn queue(&self, source: Box<dyn LiveSource>) -> Result<(), Box<dyn LiveSource>> {
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

async fn read(
    mut source: Box<dyn LiveSource>,
    queue: Arc<Mutex<Queue>>,
    tx: mpsc::Sender<FeedItem>,
) {
    let chunk_samples = CHUNK_FRAMES * usize::from(SESSION_FORMAT.channels);
    let mut pending: Vec<f32> = Vec::with_capacity(chunk_samples * 2);
    loop {
        let live = source.is_live();
        match source.next_chunk().await {
            Ok(Some(chunk)) => {
                pending.extend_from_slice(&chunk.samples);
                while pending.len() >= chunk_samples {
                    let rest = pending.split_off(chunk_samples);
                    let samples = std::mem::replace(&mut pending, rest);
                    if tx.send(FeedItem::Chunk { samples, live }).await.is_err() {
                        return;
                    }
                }
                continue;
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, "audio source failed, ending it"),
        }
        if let Some(next) = Queue::lock(&queue).next.pop_front() {
            source = next;
            continue;
        }
        if !pending.is_empty() {
            let samples = std::mem::take(&mut pending);
            if tx.send(FeedItem::Chunk { samples, live }).await.is_err() {
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
    use tas_audio::SineSource;

    use super::*;

    async fn frames(feed: &mut Feed) -> Vec<usize> {
        let mut out = Vec::new();
        while let FeedItem::Chunk { samples, .. } = feed.recv().await {
            out.push(samples.len() / 2);
        }
        out
    }

    #[tokio::test]
    async fn cuts_any_chunk_size_into_20_ms() {
        let sine = SineSource::new(SESSION_FORMAT, 440.0)
            .chunk_frames(352)
            .limit_frames(2500);
        let mut feed = Feed::spawn(Box::new(sine));
        assert_eq!(frames(&mut feed).await, [960, 960, 580]);
    }

    #[tokio::test]
    async fn a_queued_source_continues_in_the_same_chunk() {
        let a = SineSource::new(SESSION_FORMAT, 440.0).limit_frames(1000);
        let b = SineSource::new(SESSION_FORMAT, 880.0).limit_frames(1000);
        let mut feed = Feed::spawn(Box::new(a));
        feed.queue(Box::new(b)).map_err(|_| ()).unwrap();
        assert_eq!(frames(&mut feed).await, [960, 960, 80]);
        assert!(
            feed.queue(Box::new(SineSource::new(SESSION_FORMAT, 1.0)))
                .is_err()
        );
    }
}
