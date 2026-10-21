// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! A disk wrapper that limits read/write throughput (bytes/s) and IOPS.

#![forbid(unsafe_code)]

pub mod resolver;

use disk_backend::Disk;
use disk_backend::DiskError;
use disk_backend::DiskIo;
use disk_backend::UnmapBehavior;
use inspect::Inspect;
use pal_async::timer::PolledTimer;
use parking_lot::Mutex;
use scsi_buffers::RequestBuffers;
use std::time::Duration;
use std::time::Instant;
use token_bucket::TokenBucket;
use vmcore::vm_task::VmTaskDriver;
use vmcore::vm_task::VmTaskDriverSource;

/// Per-disk limits. `None` means unlimited.
#[derive(Debug, Copy, Clone, Inspect)]
pub struct RateLimits {
    /// Maximum read+write bytes per second.
    pub bytes_per_sec: Option<u64>,
    /// Maximum read+write operations per second.
    pub ops_per_sec: Option<u64>,
}

/// Allows 100ms worth of tokens to accumulate while idle.
fn burst(rate: u64) -> u64 {
    (rate / 10).max(1)
}

struct Buckets {
    bytes: Option<TokenBucket>,
    ops: Option<TokenBucket>,
}

/// A disk that delays reads and writes to stay within [`RateLimits`].
#[derive(Inspect)]
pub struct RateLimitDisk {
    limits: RateLimits,
    inner: Disk,
    #[inspect(skip)]
    buckets: Mutex<Buckets>,
    #[inspect(skip)]
    driver: VmTaskDriver,
}

impl RateLimitDisk {
    /// Wraps `inner` with the given limits.
    pub fn new(limits: RateLimits, inner: Disk, driver_source: &VmTaskDriverSource) -> Self {
        let now = Instant::now();
        let bucket = |rate: Option<u64>| {
            rate.filter(|&r| r > 0)
                .map(|r| TokenBucket::new(r, burst(r), now))
        };
        Self {
            limits,
            inner,
            buckets: Mutex::new(Buckets {
                bytes: bucket(limits.bytes_per_sec),
                ops: bucket(limits.ops_per_sec),
            }),
            driver: driver_source.current(),
        }
    }

    async fn throttle(&self, bytes: usize) {
        let wait = {
            let mut b = self.buckets.lock();
            let now = Instant::now();
            let by_bytes = b
                .bytes
                .as_mut()
                .map_or(Duration::ZERO, |x| x.reserve(now, bytes as u64));
            let by_ops = b.ops.as_mut().map_or(Duration::ZERO, |x| x.reserve(now, 1));
            by_bytes.max(by_ops)
        };
        if !wait.is_zero() {
            PolledTimer::new(&self.driver).sleep(wait).await;
        }
    }
}

impl DiskIo for RateLimitDisk {
    fn disk_type(&self) -> &str {
        "ratelimit"
    }

    fn sector_count(&self) -> u64 {
        self.inner.sector_count()
    }

    fn sector_size(&self) -> u32 {
        self.inner.sector_size()
    }

    fn disk_id(&self) -> Option<[u8; 16]> {
        self.inner.disk_id()
    }

    fn physical_sector_size(&self) -> u32 {
        self.inner.physical_sector_size()
    }

    fn is_fua_respected(&self) -> bool {
        self.inner.is_fua_respected()
    }

    fn is_read_only(&self) -> bool {
        self.inner.is_read_only()
    }

    fn pr(&self) -> Option<&dyn disk_backend::pr::PersistentReservation> {
        self.inner.pr()
    }

    async fn read_vectored(
        &self,
        buffers: &RequestBuffers<'_>,
        sector: u64,
    ) -> Result<(), DiskError> {
        self.throttle(buffers.len()).await;
        self.inner.read_vectored(buffers, sector).await
    }

    async fn write_vectored(
        &self,
        buffers: &RequestBuffers<'_>,
        sector: u64,
        fua: bool,
    ) -> Result<(), DiskError> {
        self.throttle(buffers.len()).await;
        self.inner.write_vectored(buffers, sector, fua).await
    }

    async fn sync_cache(&self) -> Result<(), DiskError> {
        self.inner.sync_cache().await
    }

    async fn wait_resize(&self, sector_count: u64) -> u64 {
        self.inner.wait_resize(sector_count).await
    }

    fn unmap(
        &self,
        sector: u64,
        count: u64,
        block_level_only: bool,
    ) -> impl Future<Output = Result<(), DiskError>> + Send {
        self.inner.unmap(sector, count, block_level_only)
    }

    fn unmap_behavior(&self) -> UnmapBehavior {
        self.inner.unmap_behavior()
    }

    fn optimal_unmap_sectors(&self) -> u32 {
        self.inner.optimal_unmap_sectors()
    }
}

#[cfg(test)]
mod tests {
    use crate::RateLimitDisk;
    use crate::RateLimits;
    use disk_backend::Disk;
    use guestmem::GuestMemory;
    use pal_async::DefaultDriver;
    use pal_async::async_test;
    use scsi_buffers::OwnedRequestBuffers;
    use std::time::Duration;
    use std::time::Instant;
    use vmcore::vm_task::SingleDriverBackend;
    use vmcore::vm_task::VmTaskDriverSource;

    fn limited(driver: DefaultDriver, limits: RateLimits) -> Disk {
        let source = VmTaskDriverSource::new(SingleDriverBackend::new(driver));
        let inner = disklayer_ram::ram_disk(4 * 1024 * 1024, false).unwrap();
        Disk::new(RateLimitDisk::new(limits, inner, &source)).unwrap()
    }

    async fn read_n(disk: &Disk, count: usize, len: usize) -> Duration {
        let mem = GuestMemory::allocate(len);
        let start = Instant::now();
        for i in 0..count {
            let sector = (i * len) as u64 / disk.sector_size() as u64;
            disk.read_vectored(
                &OwnedRequestBuffers::linear(0, len, true).buffer(&mem),
                sector,
            )
            .await
            .unwrap();
        }
        start.elapsed()
    }

    #[async_test]
    async fn sector_range_conformance(driver: DefaultDriver) {
        let disk = limited(
            driver,
            RateLimits {
                bytes_per_sec: Some(u64::MAX / 2),
                ops_per_sec: Some(u64::MAX / 2),
            },
        );
        storage_tests::sector_range::test_disk_sector_range_conformance(&disk).await;
    }

    #[async_test]
    async fn bytes_per_sec_is_enforced(driver: DefaultDriver) {
        // 4 MiB/s with a 100ms (410 KiB) burst. Eight 256 KiB reads need about
        // (2048 - 410 - 256) KiB / 4096 KiB/s = 0.34s.
        let disk = limited(
            driver,
            RateLimits {
                bytes_per_sec: Some(4 * 1024 * 1024),
                ops_per_sec: None,
            },
        );
        let elapsed = read_n(&disk, 8, 256 * 1024).await;
        assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    }

    #[async_test]
    async fn ops_per_sec_is_enforced(driver: DefaultDriver) {
        // 100 IOPS with a 10-op burst. 30 reads need about (30 - 10 - 1) / 100
        // = 0.19s.
        let disk = limited(
            driver,
            RateLimits {
                bytes_per_sec: None,
                ops_per_sec: Some(100),
            },
        );
        let elapsed = read_n(&disk, 30, 512).await;
        assert!(elapsed >= Duration::from_millis(150), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    }

    #[async_test]
    async fn no_limits_adds_no_delay(driver: DefaultDriver) {
        let disk = limited(
            driver,
            RateLimits {
                bytes_per_sec: None,
                ops_per_sec: None,
            },
        );
        let elapsed = read_n(&disk, 30, 64 * 1024).await;
        assert!(elapsed < Duration::from_millis(500), "{elapsed:?}");
    }
}
