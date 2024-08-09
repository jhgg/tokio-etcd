use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;

pub(crate) struct ExponentialBackoff {
    min: Duration,
    max: Duration,
    current: Duration,
}

impl ExponentialBackoff {
    /// Creates a new [`ExponentialBackoff`] instance with the provided minimum and maximum durations.
    pub fn new(min: Duration, max: Duration) -> Self {
        ExponentialBackoff {
            min,
            max,
            current: min,
        }
    }

    /// Calculates the next backoff duration based on the current duration.
    /// Applies a random jitter between 0 and 1 to the current duration, then doubles the duration and clamps it to the maximum allowed.
    /// Returns the calculated backoff duration.
    pub fn fail(&mut self) -> Duration {
        let jitter = rand::thread_rng().gen_range(0.0..1.0);
        let duration = self.current.mul_f64(2.0 + jitter);
        self.current = duration.min(self.max);
        self.current
    }

    pub async fn delay(&mut self) {
        sleep(self.fail()).await;
    }

    /// Resets the current backoff duration to the minimum duration, indicating that the operation was successful.
    pub fn succeed(&mut self) {
        self.current = self.min;
    }
}
