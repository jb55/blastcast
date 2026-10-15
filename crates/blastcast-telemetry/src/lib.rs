//! Telemetry: every timing a protocol measures, kept for a minute and
//! streamed live, and the state each protocol has, whole.
//!
//! **The types**, serde and nothing else, so a client that only reads them
//! (a dashboard's websocket, a native app) needs no runtime: each
//! [`Series`] with its [`Kind`] and [`Unit`], a [`Point`] of one, the
//! history window ([`WINDOW_US`]), and the status of what a protocol has
//! ([`Stream`], [`Receiver`], [`Session`], [`Speaker`]).
//!
//! **The hub**, behind the `hub` feature (on by default, and pulling in
//! tokio). A protocol crate emits into a [`Telemetry`] it is handed,
//! usually through a [`Probe`] bound to one entity (a player's client id,
//! a stream's id). A point is a sample `(series, entity, t_us, value)`, a
//! span (a stretch of the timeline, such as a chunk's play time) or a mark
//! (something that happened, such as a `stream/clear`). Which a [`Series`]
//! is, and its unit, is in the types, so a client reads them typed.
//!
//! **History.** Each series of each entity keeps a ring of its last
//! [`WINDOW_US`] of points, at most [`MAX_POINTS`], so a subscriber that
//! comes late sees the last minute at once. An entity that has emitted
//! nothing for a window is dropped when the next new one arrives.
//!
//! **Live.** [`Telemetry::subscribe`] takes the history and a receiver of
//! everything after it under one lock, so a point is in one or the other,
//! never both and never neither.
//!
//! **Cost.** A hub made with [`Telemetry::default`] is off: an emit is a
//! branch. A hub that is on, with nobody subscribed, takes an uncontended
//! lock and writes a ring; once a ring has filled its window, that
//! allocates nothing. Only an entity or series seen for the first time
//! allocates.
//!
//! **State.** A [`Roster`] is the other thing a producer hands its
//! readers: the entities of one topic it has (receivers, speakers), whole,
//! as a watch.

#[cfg(feature = "hub")]
mod hub;
mod series;
mod status;

#[cfg(feature = "hub")]
pub use hub::{Event, Keyed, MAX_POINTS, Probe, Roster, Subscription, Telemetry, Track};
pub use series::{Kind, Point, Series, Unit, WINDOW_US};
pub use status::{Format, Receiver, Session, Speaker, SpeakerSession, SpeakerState, Stream};
