use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::{broadcast, watch};

use crate::{Kind, Point, Receiver, Series, Session, Speaker, Stream, WINDOW_US};

/// The most points one series of one entity keeps, whatever its window:
/// a minute of 20 ms chunks is 3000, and a source read ahead sends faster.
pub const MAX_POINTS: usize = 4096;
/// The points a live subscriber may fall behind by before it lags and
/// takes the history again.
const LIVE_CAPACITY: usize = 8192;

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

/// An entity a [`Roster`] holds, by its id.
pub trait Keyed {
    fn key(&self) -> &str;
}

impl Keyed for Stream {
    fn key(&self) -> &str {
        &self.id
    }
}

impl Keyed for Receiver {
    fn key(&self) -> &str {
        &self.id
    }
}

impl Keyed for Session {
    fn key(&self) -> &str {
        &self.id
    }
}

impl Keyed for Speaker {
    fn key(&self) -> &str {
        &self.id
    }
}

/// The entities of one kind a producer has, by id, and a watch of them in
/// id order. A cheap clone: clones share it. A change that changes nothing
/// wakes nobody.
#[derive(Debug)]
pub struct Roster<T>(Arc<watch::Sender<Vec<T>>>);

impl<T> Clone for Roster<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Keyed + Clone + PartialEq> Default for Roster<T> {
    fn default() -> Self {
        Self(Arc::new(watch::channel(Vec::new()).0))
    }
}

impl<T: Keyed + Clone + PartialEq> Roster<T> {
    /// Every entity as of now, then each change.
    pub fn subscribe(&self) -> watch::Receiver<Vec<T>> {
        self.0.subscribe()
    }

    /// Every entity as of now.
    pub fn all(&self) -> Vec<T> {
        self.0.borrow().clone()
    }

    /// Adds `value`, or replaces the one with its id.
    pub fn set(&self, value: T) {
        self.0.send_if_modified(
            |all| match all.binary_search_by(|e| e.key().cmp(value.key())) {
                Ok(i) if all[i] == value => false,
                Ok(i) => {
                    all[i] = value;
                    true
                }
                Err(i) => {
                    all.insert(i, value);
                    true
                }
            },
        );
    }

    /// Changes the entity `id`, if there is one.
    pub fn modify(&self, id: &str, f: impl FnOnce(&mut T)) {
        self.0.send_if_modified(|all| {
            let Ok(i) = all.binary_search_by(|e| e.key().cmp(id)) else {
                return false;
            };
            let before = all[i].clone();
            f(&mut all[i]);
            all[i] != before
        });
    }

    /// Drops the entity `id`.
    pub fn remove(&self, id: &str) {
        self.retain(|e| e.key() != id);
    }

    /// Keeps only the entities `keep` picks.
    pub fn retain(&self, mut keep: impl FnMut(&T) -> bool) {
        self.0.send_if_modified(|all| {
            let before = all.len();
            all.retain(|e| keep(e));
            all.len() != before
        });
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

    fn receiver(id: &str, name: &str) -> Receiver {
        Receiver {
            id: id.into(),
            target: id.into(),
            name: name.into(),
            port: 7000,
            session: None,
            playing: false,
        }
    }

    #[test]
    fn a_roster_keeps_entities_in_id_order_and_wakes_only_on_change() {
        let roster = Roster::default();
        let mut rx = roster.subscribe();
        roster.set(receiver("b", "Den"));
        roster.set(receiver("a", "Kitchen"));
        assert!(rx.has_changed().unwrap());
        let names: Vec<String> = rx
            .borrow_and_update()
            .iter()
            .map(|r| r.name.clone())
            .collect();
        assert_eq!(names, ["Kitchen", "Den"]);
        roster.set(receiver("a", "Kitchen"));
        roster.modify("a", |r| r.name = "Kitchen".into());
        roster.modify("nobody", |r| r.name = "x".into());
        roster.remove("nobody");
        assert!(!rx.has_changed().unwrap(), "nothing changed");
        roster.modify("a", |r| r.playing = true);
        assert!(rx.borrow_and_update()[0].playing);
        roster.remove("a");
        assert_eq!(rx.borrow_and_update().len(), 1);
        assert_eq!(roster.all()[0].id, "b");
    }
}
