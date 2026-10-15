//! Emitting on the audio path allocates nothing, counted by a global
//! allocator that counts this thread's allocations: with the hub off, and
//! with it on once each ring has filled its window, subscribed or not.

use blastcast_telemetry::{Probe, Series, Telemetry, WINDOW_US};

/// A chunk's worth of points, as a Sendspin player gets them.
fn chunk(probe: &Probe, t_us: i64) {
    probe.sample(Series::Lead, t_us, 250_000.0);
    probe.sample(Series::Buffered, t_us, 240_000.0);
    probe.span(Series::ChunkSent, t_us, t_us + 250_000, 20_000);
}

/// Runs `chunks` 20 ms chunks to each probe, from `from_us`.
fn run(probes: &[Probe], from_us: i64, chunks: i64) -> i64 {
    let mut t = from_us;
    for _ in 0..chunks {
        for p in probes {
            chunk(p, t);
        }
        t += 20_000;
    }
    t
}

#[test]
fn a_hub_that_is_off_never_allocates() {
    let probe = Probe::default();
    let info = allocation_counter::measure(|| {
        run(std::slice::from_ref(&probe), 0, 1000);
    });
    assert_eq!(info.count_total, 0);
}

#[test]
fn a_full_ring_takes_points_without_allocating() {
    let hub = Telemetry::new();
    let probes = [hub.probe("kitchen"), hub.probe("attic")];
    // The counter counts: an entity's first point makes its rings.
    let first = allocation_counter::measure(|| chunk(&probes[0], 0));
    assert!(first.count_total > 0);
    // A window and a bit, so every ring is as big as it gets.
    let t = run(&probes, 0, WINDOW_US / 20_000 + 100);
    let info = allocation_counter::measure(|| {
        run(&probes, t, 5000);
    });
    assert_eq!(info.count_total, 0, "{info:?}");
}

#[test]
fn a_subscriber_costs_no_allocation_per_point() {
    let hub = Telemetry::new();
    let probes = [hub.probe("kitchen")];
    let t = run(&probes, 0, WINDOW_US / 20_000 + 100);
    let mut sub = hub.subscribe(t);
    let info = allocation_counter::measure(|| {
        run(&probes, t, 1000);
    });
    assert_eq!(info.count_total, 0, "{info:?}");
    assert_eq!(sub.live.try_recv().unwrap().point.t_us, t);
}
