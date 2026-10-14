//! The telemetry hub: every timing tas measures, kept for a minute and
//! streamed live, for `/v1/ws` and the dashboard.
//!
//! A protocol crate emits into a [`Telemetry`] it is handed, usually through
//! a [`Probe`] bound to one entity (a player's client id, a stream's id).
//! A point is a sample `(series, entity, t_us, value)`, a span (a stretch
//! of the timeline, such as a chunk's play time) or a mark (something that
//! happened, such as a `stream/clear`). Which a [`Series`] is, and its unit,
//! is in tas-proto, so a native client reads them typed.
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

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tas_proto::{Column, Kind, Series, TelemetryFilter};
use tokio::sync::broadcast;

/// How far back a series' history goes: a minute, on the server clock.
pub const WINDOW_US: i64 = 60_000_000;
/// The most points one series of one entity keeps, whatever its window:
/// a minute of 20 ms chunks is 3000, and a source read ahead sends faster.
pub const MAX_POINTS: usize = 4096;
/// The points a live subscriber may fall behind by before it lags and
/// takes the history again.
const LIVE_CAPACITY: usize = 8192;

/// One point of a series. A sample and a mark use `value` alone; a span
/// plays from `value` (server-clock µs) for `duration_us`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    /// When it was measured, in server-clock µs.
    pub t_us: i64,
    pub value: f64,
    /// A span's length; 0 for anything else.
    pub duration_us: i64,
}

/// A point as it is emitted, to a live subscriber.
#[derive(Debug, Clone)]
pub struct Event {
    pub series: Series,
    pub entity: Arc<str>,
    pub point: Point,
}

/// One series of one entity's history, oldest first.
#[derive(Debug, Clone, PartialEq)]
pub struct Track {
    pub series: Series,
    pub entity: Arc<str>,
    pub points: Vec<Point>,
}

/// What [`Telemetry::subscribe`] returns.
#[derive(Debug)]
pub struct Subscription {
    /// Every series of every entity as of the subscribe, within the window.
    pub history: Vec<Track>,
    /// Every point emitted after it. Closed at once for a hub that is off.
    pub live: broadcast::Receiver<Event>,
}

/// The hub. A cheap clone: clones share it. [`Telemetry::default`] is a hub
/// that is off and keeps nothing.
#[derive(Clone, Default)]
pub struct Telemetry(Option<Arc<Hub>>);

impl std::fmt::Debug for Telemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Telemetry").field(&self.is_on()).finish()
    }
}

struct Hub {
    state: Mutex<State>,
    live: broadcast::Sender<Event>,
}

#[derive(Default)]
struct State {
    entities: HashMap<Arc<str>, Entity>,
}

struct Entity {
    /// Its key, cloned into each live [`Event`] without allocating.
    name: Arc<str>,
    /// A handful per entity, so a scan beats a map.
    series: Vec<(Series, VecDeque<Point>)>,
    newest_us: i64,
}

impl Telemetry {
    /// A hub that is on.
    pub fn new() -> Self {
        Self(Some(Arc::new(Hub {
            state: Mutex::new(State::default()),
            live: broadcast::channel(LIVE_CAPACITY).0,
        })))
    }

    pub fn is_on(&self) -> bool {
        self.0.is_some()
    }

    /// A handle that emits for `entity`.
    pub fn probe(&self, entity: &str) -> Probe {
        Probe {
            hub: self.clone(),
            entity: entity.into(),
        }
    }

    /// `series` of `entity` was `value` at `t_us`.
    pub fn sample(&self, series: Series, entity: &str, t_us: i64, value: f64) {
        debug_assert_eq!(series.kind(), Kind::Sample, "{series:?}");
        self.emit(series, entity, point(t_us, value, 0));
    }

    /// At `t_us`, `entity` had `series` cover the timeline from `start_us`
    /// for `duration_us`.
    pub fn span(&self, series: Series, entity: &str, t_us: i64, start_us: i64, duration_us: i64) {
        debug_assert_eq!(series.kind(), Kind::Span, "{series:?}");
        self.emit(series, entity, point(t_us, start_us as f64, duration_us));
    }

    /// The `kind` of thing happened to `entity` at `t_us`; `detail` is in
    /// the kind's unit.
    pub fn mark(&self, kind: Series, entity: &str, t_us: i64, detail: f64) {
        debug_assert_eq!(kind.kind(), Kind::Mark, "{kind:?}");
        self.emit(kind, entity, point(t_us, detail, 0));
    }

    fn emit(&self, series: Series, entity: &str, point: Point) {
        let Some(hub) = &self.0 else {
            return;
        };
        let mut state = hub.lock();
        if !state.entities.contains_key(entity) {
            state.add(entity, point.t_us);
        }
        let e = state.entities.get_mut(entity).expect("added above");
        let ring = match e.series.iter().position(|(s, _)| *s == series) {
            Some(i) => &mut e.series[i].1,
            None => {
                e.series.push((series, VecDeque::new()));
                &mut e.series.last_mut().expect("pushed").1
            }
        };
        let oldest = point.t_us - WINDOW_US;
        while ring.len() >= MAX_POINTS || ring.front().is_some_and(|p| p.t_us < oldest) {
            ring.pop_front();
        }
        ring.push_back(point);
        e.newest_us = e.newest_us.max(point.t_us);
        // Under the lock, so a subscribe sees each point once.
        if hub.live.receiver_count() > 0 {
            let _ = hub.live.send(Event {
                series,
                entity: e.name.clone(),
                point,
            });
        }
    }

    /// The history as of `now_us`, and a receiver of every point after it.
    pub fn subscribe(&self, now_us: i64) -> Subscription {
        let Some(hub) = &self.0 else {
            let (_, live) = broadcast::channel(1);
            return Subscription {
                history: Vec::new(),
                live,
            };
        };
        let state = hub.lock();
        let oldest = now_us - WINDOW_US;
        let mut history = Vec::new();
        for e in state.entities.values() {
            for (series, ring) in &e.series {
                let first = ring.partition_point(|p| p.t_us < oldest);
                if first < ring.len() {
                    history.push(Track {
                        series: *series,
                        entity: e.name.clone(),
                        points: ring.range(first..).copied().collect(),
                    });
                }
            }
        }
        history.sort_by(|a, b| (&a.entity, a.series).cmp(&(&b.entity, b.series)));
        Subscription {
            history,
            live: hub.live.subscribe(),
        }
    }
}

impl Hub {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl State {
    /// Adds `entity`, first dropping every one silent for a window before
    /// `t_us`: streams that ended, players that left.
    fn add(&mut self, entity: &str, t_us: i64) {
        self.entities.retain(|_, e| e.newest_us >= t_us - WINDOW_US);
        let name: Arc<str> = entity.into();
        self.entities.insert(
            name.clone(),
            Entity {
                name,
                series: Vec::new(),
                newest_us: t_us,
            },
        );
    }
}

fn point(t_us: i64, value: f64, duration_us: i64) -> Point {
    Point {
        t_us,
        value,
        duration_us,
    }
}

/// A [`Telemetry`] bound to one entity. A cheap clone.
#[derive(Debug, Clone)]
pub struct Probe {
    hub: Telemetry,
    entity: Arc<str>,
}

impl Default for Probe {
    /// A probe into a hub that is off.
    fn default() -> Self {
        Telemetry::default().probe("")
    }
}

impl Probe {
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// See [`Telemetry::sample`].
    pub fn sample(&self, series: Series, t_us: i64, value: f64) {
        self.hub.sample(series, &self.entity, t_us, value);
    }

    /// See [`Telemetry::span`].
    pub fn span(&self, series: Series, t_us: i64, start_us: i64, duration_us: i64) {
        self.hub
            .span(series, &self.entity, t_us, start_us, duration_us);
    }

    /// See [`Telemetry::mark`].
    pub fn mark(&self, kind: Series, t_us: i64, detail: f64) {
        self.hub.mark(kind, &self.entity, t_us, detail);
    }
}

/// Points gathered into tas-proto's columns: one per series and entity,
/// in the order each first came.
#[derive(Debug, Default)]
pub struct Columns(Vec<Column>);

impl Columns {
    /// The `history` that `filter` picks.
    pub fn from_history(history: &[Track], filter: &TelemetryFilter) -> Self {
        let mut columns = Self::default();
        for track in history {
            if filter.matches(track.series, &track.entity) {
                for p in &track.points {
                    columns.push(track.series, &track.entity, *p);
                }
            }
        }
        columns
    }

    pub fn push(&mut self, series: Series, entity: &str, p: Point) {
        let i = match self
            .0
            .iter()
            .position(|c| c.series == series && c.entity == entity)
        {
            Some(i) => i,
            None => {
                self.0.push(Column {
                    series,
                    entity: entity.into(),
                    t: Vec::new(),
                    v: Vec::new(),
                    d: Vec::new(),
                });
                self.0.len() - 1
            }
        };
        let c = &mut self.0[i];
        c.t.push(p.t_us);
        c.v.push(p.value);
        if series.kind() == Kind::Span {
            c.d.push(p.duration_us);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The columns so far, leaving none.
    pub fn take(&mut self) -> Vec<Column> {
        std::mem::take(&mut self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(track: &Track) -> Vec<f64> {
        track.points.iter().map(|p| p.value).collect()
    }

    #[test]
    fn a_hub_that_is_off_keeps_nothing() {
        let hub = Telemetry::default();
        hub.sample(Series::Lead, "p1", 0, 1.0);
        let sub = hub.subscribe(0);
        assert!(sub.history.is_empty());
        assert!(Probe::default().entity().is_empty());
    }

    #[test]
    fn history_is_the_last_window_per_series_and_entity() {
        let hub = Telemetry::new();
        let p1 = hub.probe("p1");
        for i in 0..5 {
            p1.sample(Series::Lead, i * 20_000_000, i as f64);
        }
        p1.sample(Series::Rtt, 0, 7.0);
        hub.probe("s1")
            .mark(Series::TimelineShift, 80_000_000, 30_000.0);
        // At 80 s: lead from 20 s on, the rtt at 0 s too old.
        let sub = hub.subscribe(80_000_000);
        let tracks: Vec<(&str, Series, Vec<f64>)> = sub
            .history
            .iter()
            .map(|t| (&*t.entity, t.series, values(t)))
            .collect();
        assert_eq!(
            tracks,
            vec![
                ("p1", Series::Lead, vec![1.0, 2.0, 3.0, 4.0]),
                ("s1", Series::TimelineShift, vec![30_000.0]),
            ]
        );
    }

    #[test]
    fn a_ring_keeps_at_most_max_points() {
        let hub = Telemetry::new();
        for i in 0..MAX_POINTS as i64 + 10 {
            hub.sample(Series::Lead, "p1", i, i as f64);
        }
        let sub = hub.subscribe(0);
        let points = &sub.history[0].points;
        assert_eq!(points.len(), MAX_POINTS);
        assert_eq!(points[0].t_us, 10);
    }

    #[test]
    fn a_silent_entity_goes_when_a_new_one_comes() {
        let hub = Telemetry::new();
        hub.sample(Series::Lead, "old", 0, 1.0);
        hub.sample(Series::Lead, "kept", 30_000_000, 1.0);
        hub.sample(Series::Lead, "new", WINDOW_US + 1, 1.0);
        let state = hub.0.as_ref().unwrap().lock();
        let mut names: Vec<&str> = state.entities.keys().map(|k| &**k).collect();
        names.sort();
        assert_eq!(names, ["kept", "new"]);
    }

    #[tokio::test]
    async fn a_late_subscriber_gets_history_then_live_without_gap_or_repeat() {
        let hub = Telemetry::new();
        let p1 = hub.probe("p1");
        p1.sample(Series::Lead, 1, 1.0);
        p1.sample(Series::Lead, 2, 2.0);
        let mut sub = hub.subscribe(2);
        p1.sample(Series::Lead, 3, 3.0);
        p1.span(Series::ChunkSent, 4, 250_004, 20_000);
        assert_eq!(values(&sub.history[0]), [1.0, 2.0]);
        let e = sub.live.recv().await.unwrap();
        assert_eq!(
            (e.series, &*e.entity, e.point.value),
            (Series::Lead, "p1", 3.0)
        );
        let e = sub.live.recv().await.unwrap();
        assert_eq!(
            (e.series, e.point),
            (
                Series::ChunkSent,
                Point {
                    t_us: 4,
                    value: 250_004.0,
                    duration_us: 20_000
                }
            )
        );
    }

    #[test]
    fn columns_group_by_series_and_entity_and_only_spans_carry_durations() {
        let hub = Telemetry::new();
        hub.sample(Series::Lead, "p1", 1, 10.0);
        hub.sample(Series::Lead, "p2", 1, 20.0);
        hub.span(Series::ChunkSent, "p1", 1, 100, 20);
        hub.sample(Series::Lead, "p1", 2, 11.0);
        let history = hub.subscribe(2).history;
        let filter = TelemetryFilter {
            series: Vec::new(),
            entities: vec!["p1".into()],
        };
        let mut columns = Columns::from_history(&history, &filter);
        let columns = columns.take();
        assert_eq!(columns.len(), 2);
        let lead = columns.iter().find(|c| c.series == Series::Lead).unwrap();
        assert_eq!((&lead.t[..], &lead.v[..]), (&[1, 2][..], &[10.0, 11.0][..]));
        assert!(lead.d.is_empty());
        let sent = columns
            .iter()
            .find(|c| c.series == Series::ChunkSent)
            .unwrap();
        assert_eq!((&sent.v[..], &sent.d[..]), (&[100.0][..], &[20][..]));
    }
}
