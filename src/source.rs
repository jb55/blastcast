//! Where a session's audio comes from: a [`LiveSource`] is pulled for PCM
//! chunks by the session that plays it.
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
