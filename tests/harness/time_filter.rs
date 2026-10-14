//! The client side of Sendspin clock sync: a player's own clock and the
//! time filter it runs on `server/time` replies.

use tokio::time::Instant;

/// sendspin-go's `pkg/sync/timefilter.go`: a two-dimensional Kalman filter
/// over clock offset and drift, with the upstream time-filter defaults.
/// Times are in microseconds; the offset is server minus client.
pub struct TimeFilter {
    last_update: i64,
    pub offset: f64,
    drift: f64,
    offset_cov: f64,
    offset_drift_cov: f64,
    drift_cov: f64,
    use_drift: bool,
    count: u32,
}

impl TimeFilter {
    const PROCESS_VARIANCE: f64 = 0.0;
    const DRIFT_PROCESS_VARIANCE: f64 = 1e-11 * 1e-11;
    const FORGET_VARIANCE_FACTOR: f64 = 2.0 * 2.0;
    const ADAPTIVE_CUTOFF: f64 = 3.0;
    const MIN_SAMPLES: u32 = 100;
    const DRIFT_SIGNIFICANCE_SQ: f64 = 2.0 * 2.0;
    const MAX_ERROR_SCALE: f64 = 0.5;

    pub fn new() -> Self {
        Self {
            last_update: 0,
            offset: 0.0,
            drift: 0.0,
            offset_cov: f64::INFINITY,
            offset_drift_cov: 0.0,
            drift_cov: 0.0,
            use_drift: false,
            count: 0,
        }
    }

    /// `measurement` is ((T2-T1)+(T3-T4))/2, `max_error` ((T4-T1)-(T3-T2))/2,
    /// `time_added` the client's T4.
    pub fn update(&mut self, measurement: i64, max_error: i64, time_added: i64) {
        if time_added <= self.last_update {
            return;
        }
        let dt = (time_added - self.last_update) as f64;
        let dt_sq = dt * dt;
        self.last_update = time_added;
        let meas_var = (max_error as f64 * Self::MAX_ERROR_SCALE).powi(2);
        let measurement = measurement as f64;

        if self.count == 0 {
            self.count += 1;
            self.offset = measurement;
            self.offset_cov = meas_var;
            self.drift = 0.0;
            return;
        }
        if self.count == 1 {
            self.count += 1;
            self.drift = (measurement - self.offset) / dt;
            self.offset = measurement;
            self.drift_cov = (self.offset_cov + meas_var) / dt_sq;
            self.offset_cov = meas_var;
            return;
        }

        let pred_offset = self.offset + self.drift * dt;
        let mut new_drift_cov = self.drift_cov + dt * Self::DRIFT_PROCESS_VARIANCE;
        let mut new_offset_drift_cov = self.offset_drift_cov + self.drift_cov * dt;
        let mut new_offset_cov = self.offset_cov
            + 2.0 * self.offset_drift_cov * dt
            + self.drift_cov * dt_sq
            + dt * Self::PROCESS_VARIANCE;

        let residual = measurement - pred_offset;
        let cutoff = max_error as f64 * Self::ADAPTIVE_CUTOFF;
        if self.count < Self::MIN_SAMPLES {
            self.count += 1;
        } else if residual.abs() > cutoff {
            new_drift_cov *= Self::FORGET_VARIANCE_FACTOR;
            new_offset_drift_cov *= Self::FORGET_VARIANCE_FACTOR;
            new_offset_cov *= Self::FORGET_VARIANCE_FACTOR;
        }

        let inv_s = 1.0 / (new_offset_cov + meas_var);
        let offset_gain = new_offset_cov * inv_s;
        let drift_gain = new_offset_drift_cov * inv_s;
        self.offset = pred_offset + offset_gain * residual;
        self.drift += drift_gain * residual;
        self.drift_cov = new_drift_cov - drift_gain * new_offset_drift_cov;
        self.offset_drift_cov = new_offset_drift_cov - drift_gain * new_offset_cov;
        self.offset_cov = new_offset_cov - offset_gain * new_offset_cov;
        self.use_drift = self.drift * self.drift > Self::DRIFT_SIGNIFICANCE_SQ * self.drift_cov;
    }

    /// A server timestamp on the client's clock, as a player schedules a
    /// chunk.
    pub fn compute_client_time(&self, server_time: i64) -> i64 {
        let drift = if self.use_drift { self.drift } else { 0.0 };
        ((server_time as f64 - self.offset + drift * self.last_update as f64) / (1.0 + drift))
            .round() as i64
    }

    /// The estimated standard deviation of the offset.
    pub fn error_us(&self) -> f64 {
        self.offset_cov.sqrt()
    }
}

/// The client's own monotonic clock, deliberately far from the server's.
#[derive(Clone, Copy)]
pub struct ClientClock {
    pub epoch: Instant,
    pub base_us: i64,
}

impl ClientClock {
    pub fn now_us(&self) -> i64 {
        self.base_us + i64::try_from(self.epoch.elapsed().as_micros()).unwrap()
    }
}
