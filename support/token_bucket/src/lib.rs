// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! A token bucket for rate limiting device I/O.

#![forbid(unsafe_code)]

use std::time::Duration;
use std::time::Instant;

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// A token bucket that allows debt.
///
/// Tokens refill continuously at `rate` per second up to `burst`. A caller is
/// admitted whenever the bucket is not in debt, and its tokens are then
/// subtracted even if that drives the bucket negative. Later callers wait until
/// the debt is repaid. This admits requests larger than the burst without
/// starving them, while the long-run rate still matches `rate`.
///
/// Internally the balance is kept in token-nanoseconds so refill is exact.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    rate: u64,
    burst_ns: i128,
    balance_ns: i128,
    last: Instant,
}

impl TokenBucket {
    /// Returns a full bucket refilling at `rate` tokens per second, holding at
    /// most `burst` tokens.
    ///
    /// Panics if `rate` is zero. Callers model "unlimited" by not having a
    /// bucket.
    pub fn new(rate: u64, burst: u64, now: Instant) -> Self {
        assert!(rate > 0, "token bucket rate must be non-zero");
        let burst_ns = burst as i128 * NANOS_PER_SEC as i128;
        Self {
            rate,
            burst_ns,
            balance_ns: burst_ns,
            last: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last);
        self.last = self.last.max(now);
        let added = elapsed.as_nanos().saturating_mul(self.rate as u128);
        self.balance_ns = self
            .balance_ns
            .saturating_add(added.min(i128::MAX as u128) as i128)
            .min(self.burst_ns);
    }

    fn wait(&self) -> Option<Duration> {
        if self.balance_ns >= 0 {
            return None;
        }
        let debt = self.balance_ns.unsigned_abs();
        let nanos = debt.div_ceil(self.rate as u128);
        Some(Duration::from_nanos(nanos.min(u64::MAX as u128) as u64))
    }

    /// Returns how long to wait before the bucket is out of debt, or `None` if
    /// it can admit a caller now. Does not consume tokens.
    pub fn ready(&mut self, now: Instant) -> Option<Duration> {
        self.refill(now);
        self.wait()
    }

    /// Subtracts `n` tokens, possibly going into debt.
    pub fn consume(&mut self, now: Instant, n: u64) {
        self.refill(now);
        self.balance_ns -= n as i128 * NANOS_PER_SEC as i128;
    }

    /// Returns `n` previously consumed tokens, never exceeding the burst.
    pub fn refund(&mut self, n: u64) {
        self.balance_ns = self
            .balance_ns
            .saturating_add(n as i128 * NANOS_PER_SEC as i128)
            .min(self.burst_ns);
    }

    /// Books `n` tokens and returns how long the caller must wait before
    /// issuing its request.
    pub fn reserve(&mut self, now: Instant, n: u64) -> Duration {
        self.refill(now);
        let wait = self.wait().unwrap_or(Duration::ZERO);
        self.balance_ns -= n as i128 * NANOS_PER_SEC as i128;
        wait
    }
}

#[cfg(test)]
mod tests {
    use super::TokenBucket;
    use std::time::Duration;
    use std::time::Instant;

    const MS: Duration = Duration::from_millis(1);

    #[test]
    fn full_bucket_admits_burst_without_wait() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(1000, 100, t0);
        assert_eq!(b.reserve(t0, 100), Duration::ZERO);
    }

    #[test]
    fn debt_is_repaid_at_rate() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(1000, 100, t0);
        assert_eq!(b.reserve(t0, 100), Duration::ZERO);
        // The bucket is at zero, not in debt, so this is admitted immediately.
        assert_eq!(b.reserve(t0, 50), Duration::ZERO);
        // Now 50 tokens of debt: the next caller waits 50ms.
        assert_eq!(b.reserve(t0, 10), 50 * MS);
    }

    #[test]
    fn oversized_request_is_admitted_and_later_callers_wait() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(1000, 100, t0);
        assert_eq!(b.reserve(t0, 1000), Duration::ZERO);
        assert_eq!(b.reserve(t0, 1), 900 * MS);
    }

    #[test]
    fn refill_is_capped_at_burst() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(1000, 100, t0);
        assert_eq!(b.reserve(t0, 100), Duration::ZERO);
        let later = t0 + Duration::from_secs(10);
        assert_eq!(b.reserve(later, 100), Duration::ZERO);
        assert_eq!(b.reserve(later, 1), Duration::ZERO);
        assert_eq!(b.reserve(later, 1), MS);
    }

    #[test]
    fn ready_reports_wait_without_consuming() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(1000, 100, t0);
        assert_eq!(b.ready(t0), None);
        b.consume(t0, 150);
        assert_eq!(b.ready(t0), Some(50 * MS));
        assert_eq!(b.ready(t0), Some(50 * MS));
        assert_eq!(b.ready(t0 + 50 * MS), None);
    }

    #[test]
    fn refund_never_exceeds_burst() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(1000, 100, t0);
        b.consume(t0, 30);
        b.refund(1000);
        b.consume(t0, 100);
        assert_eq!(b.ready(t0), None);
        b.consume(t0, 1);
        assert_eq!(b.ready(t0), Some(MS));
    }

    #[test]
    fn sustained_rate_matches_configured_rate() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new(10_000, 1_000, t0);
        let mut now = t0;
        for _ in 0..100 {
            now += b.reserve(now, 1000);
        }
        // 100k tokens at 10k/s with a 1k burst: the first two chunks go out at
        // once (full bucket, then zero), each later one waits 100ms.
        assert_eq!(now - t0, Duration::from_millis(9800));
    }
}
