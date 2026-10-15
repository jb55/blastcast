//! Where a session's audio comes from: a [`LiveSource`] is pulled for PCM
//! chunks by the session that plays it, and a [`TimedSource`] for chunks
//! that may say when they play, with the flushes between them.
//!
//! A *live* source (AirPlay, a paced test tone) produces audio in real time
//! and can't be read ahead, so the scheduler adds no lead beyond the
//! players' own buffer. A source that isn't live can be read as fast as the
//! scheduler likes.

use async_trait::async_trait;

use crate::AudioError;
use crate::format::{AudioFormat, PcmChunk};

#[async_trait]
pub trait LiveSource: Send {
    /// The format of every chunk this source yields.
    fn format(&self) -> AudioFormat;

    /// The format the audio arrived in, before any conversion: what a
    /// [`format`](Self::format)-changing wrapper, such as a resampler, was
    /// given. Only shown, as [`kind`](Self::kind) is.
    fn source_format(&self) -> AudioFormat {
        self.format()
    }

    /// Whether audio arrives in real time. A live source gets no extra lead.
    fn is_live(&self) -> bool;

    /// Where the audio comes from, for whoever watches the stream:
    /// `airplay`, `tone`. Only shown, never matched on.
    fn kind(&self) -> &'static str {
        "unknown"
    }

    /// The next chunk, or `None` when the stream has ended. Chunks may be
    /// any size, but never empty.
    async fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError>;
}

#[async_trait]
impl<S: LiveSource + ?Sized> LiveSource for Box<S> {
    fn format(&self) -> AudioFormat {
        (**self).format()
    }

    fn source_format(&self) -> AudioFormat {
        (**self).source_format()
    }

    fn is_live(&self) -> bool {
        (**self).is_live()
    }

    fn kind(&self) -> &'static str {
        (**self).kind()
    }

    async fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError> {
        (**self).next_chunk().await
    }
}

/// What a [`TimedSource`] yields: audio, or an event that must stay in
/// order with it. Volume and metadata are not here: they don't need to
/// land between two particular frames, so they travel out of band.
#[derive(Debug, Clone, PartialEq)]
pub enum Item {
    /// `play_at_us` is when the chunk's first frame plays, on the
    /// [`ServerClock`](crate::ServerClock); `None` means the sink places
    /// it, as it would a [`LiveSource`]'s.
    Chunk {
        pcm: PcmChunk,
        play_at_us: Option<i64>,
    },
    /// Drop everything not yet played; the next chunk places the timeline
    /// afresh.
    Flush,
    End,
}

/// A source that may say when its audio plays: AirPlay, whose sender
/// stamps every packet. Every [`LiveSource`] is one through [`Untimed`],
/// whose chunks the sink places itself.
#[async_trait]
pub trait TimedSource: Send {
    /// The format of every chunk this source yields.
    fn format(&self) -> AudioFormat;

    /// As [`LiveSource::source_format`].
    fn source_format(&self) -> AudioFormat {
        self.format()
    }

    /// As [`LiveSource::is_live`]. A source that stamps its audio with play
    /// times produces it in real time.
    fn is_live(&self) -> bool {
        true
    }

    /// As [`LiveSource::kind`].
    fn kind(&self) -> &'static str {
        "unknown"
    }

    /// The next item. `Ok(None)` ends the stream as [`Item::End`] does.
    /// Chunks may be any size, but never empty.
    async fn next(&mut self) -> Result<Option<Item>, AudioError>;
}

#[async_trait]
impl<S: TimedSource + ?Sized> TimedSource for Box<S> {
    fn format(&self) -> AudioFormat {
        (**self).format()
    }

    fn source_format(&self) -> AudioFormat {
        (**self).source_format()
    }

    fn is_live(&self) -> bool {
        (**self).is_live()
    }

    fn kind(&self) -> &'static str {
        (**self).kind()
    }

    async fn next(&mut self) -> Result<Option<Item>, AudioError> {
        (**self).next().await
    }
}

/// A [`LiveSource`] as a [`TimedSource`]: each chunk unstamped, and the
/// end as [`Item::End`].
pub struct Untimed<S>(pub S);

#[async_trait]
impl<S: LiveSource> TimedSource for Untimed<S> {
    fn format(&self) -> AudioFormat {
        self.0.format()
    }

    fn source_format(&self) -> AudioFormat {
        self.0.source_format()
    }

    fn is_live(&self) -> bool {
        self.0.is_live()
    }

    fn kind(&self) -> &'static str {
        self.0.kind()
    }

    async fn next(&mut self) -> Result<Option<Item>, AudioError> {
        Ok(Some(match self.0.next_chunk().await? {
            Some(pcm) => Item::Chunk {
                pcm,
                play_at_us: None,
            },
            None => Item::End,
        }))
    }
}

/// Whatever a sink taking a [`TimedSource`] can be handed: a boxed
/// [`LiveSource`], through [`Untimed`], or a boxed `dyn TimedSource`.
pub trait IntoTimed {
    fn into_timed(self) -> Box<dyn TimedSource>;
}

impl<S: LiveSource + ?Sized + 'static> IntoTimed for Box<S> {
    fn into_timed(self) -> Box<dyn TimedSource> {
        Box::new(Untimed(self))
    }
}

impl IntoTimed for Box<dyn TimedSource> {
    fn into_timed(self) -> Box<dyn TimedSource> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two chunks of silence, then the end.
    struct Two(u8);

    #[async_trait]
    impl LiveSource for Two {
        fn format(&self) -> AudioFormat {
            AudioFormat::new(48_000, 2)
        }

        fn is_live(&self) -> bool {
            false
        }

        fn kind(&self) -> &'static str {
            "two"
        }

        async fn next_chunk(&mut self) -> Result<Option<PcmChunk>, AudioError> {
            if self.0 == 2 {
                return Ok(None);
            }
            self.0 += 1;
            Ok(Some(PcmChunk::new(self.format(), vec![0.0; 4])))
        }
    }

    #[tokio::test]
    async fn a_live_source_is_an_unstamped_timed_one() {
        let mut timed = (Box::new(Two(0)) as Box<dyn LiveSource>).into_timed();
        assert!(!timed.is_live());
        assert_eq!(timed.kind(), "two");
        for _ in 0..2 {
            let item = timed.next().await.unwrap().unwrap();
            assert!(matches!(
                item,
                Item::Chunk {
                    play_at_us: None,
                    ..
                }
            ));
        }
        assert_eq!(timed.next().await.unwrap(), Some(Item::End));
    }
}
