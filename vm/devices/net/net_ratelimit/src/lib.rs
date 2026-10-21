// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! A network endpoint wrapper that limits bytes/s and packets/s in each
//! direction, shared across all queues of the NIC.

#![forbid(unsafe_code)]

pub mod resolver;

use async_trait::async_trait;
use guestmem::GuestMemory;
use inspect::Inspect;
use inspect::InspectMut;
use net_backend::BackendQueueStats;
use net_backend::BufferAccess;
use net_backend::Endpoint;
use net_backend::EndpointAction;
use net_backend::MultiQueueSupport;
use net_backend::Queue;
use net_backend::QueueConfig;
use net_backend::RssConfig;
use net_backend::RxBufferSegment;
use net_backend::RxId;
use net_backend::RxMetadata;
use net_backend::TxError;
use net_backend::TxId;
use net_backend::TxOffloadSupport;
use net_backend::TxSegment;
use net_backend::next_packet;
use pal_async::timer::PolledTimer;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use token_bucket::TokenBucket;

/// Per-NIC limits, applied separately to transmit and receive. `None` means
/// unlimited.
#[derive(Debug, Copy, Clone, Inspect)]
pub struct NetRateLimits {
    /// Maximum bytes per second in each direction.
    pub bytes_per_sec: Option<u64>,
    /// Maximum packets per second in each direction.
    pub packets_per_sec: Option<u64>,
}

/// Allows 100ms worth of tokens to accumulate while idle.
fn burst(rate: u64) -> u64 {
    (rate / 10).max(1)
}

struct Direction {
    bytes: Option<TokenBucket>,
    packets: Option<TokenBucket>,
}

impl Direction {
    fn new(limits: &NetRateLimits, now: Instant) -> Self {
        let bucket = |rate: Option<u64>| {
            rate.filter(|&r| r > 0)
                .map(|r| TokenBucket::new(r, burst(r), now))
        };
        Self {
            bytes: bucket(limits.bytes_per_sec),
            packets: bucket(limits.packets_per_sec),
        }
    }

    fn unlimited(&self) -> bool {
        self.bytes.is_none() && self.packets.is_none()
    }

    fn ready(&mut self, now: Instant) -> Option<Duration> {
        let a = self.bytes.as_mut().and_then(|b| b.ready(now));
        let b = self.packets.as_mut().and_then(|b| b.ready(now));
        a.max(b)
    }

    fn consume(&mut self, now: Instant, bytes: u64, packets: u64) {
        if let Some(b) = &mut self.bytes {
            b.consume(now, bytes);
        }
        if let Some(b) = &mut self.packets {
            b.consume(now, packets);
        }
    }

    fn refund(&mut self, bytes: u64, packets: u64) {
        if let Some(b) = &mut self.bytes {
            b.refund(bytes);
        }
        if let Some(b) = &mut self.packets {
            b.refund(packets);
        }
    }
}

/// Returns how many packets `meta` becomes on the wire. With TCP/UDP
/// segmentation offload one transmit can carry many MSS-sized segments, and a
/// packets/s limit is meaningless unless it counts those.
fn wire_packets(meta: &net_backend::TxMetadata) -> u64 {
    let flags = meta.flags;
    if !(flags.offload_tcp_segmentation() || flags.offload_udp_segmentation())
        || meta.max_segment_size == 0
    {
        return 1;
    }
    let headers = meta.l2_len as u32 + meta.l3_len as u32 + meta.l4_len as u32;
    let payload = meta.len.saturating_sub(headers);
    u64::from(payload.div_ceil(meta.max_segment_size.into()).max(1))
}

struct Shared {
    tx: Direction,
    rx: Direction,
}

/// An endpoint that rate limits another endpoint.
#[derive(InspectMut)]
pub struct RateLimitEndpoint {
    #[inspect(mut)]
    inner: Box<dyn Endpoint>,
    limits: NetRateLimits,
    #[inspect(skip)]
    shared: Arc<Mutex<Shared>>,
}

impl RateLimitEndpoint {
    /// Wraps `inner` with the given limits.
    pub fn new(inner: Box<dyn Endpoint>, limits: NetRateLimits) -> Self {
        let now = Instant::now();
        Self {
            inner,
            limits,
            shared: Arc::new(Mutex::new(Shared {
                tx: Direction::new(&limits, now),
                rx: Direction::new(&limits, now),
            })),
        }
    }
}

#[async_trait]
impl Endpoint for RateLimitEndpoint {
    fn endpoint_type(&self) -> &'static str {
        "ratelimit"
    }

    async fn get_queues(
        &mut self,
        config: Vec<QueueConfig>,
        rss: Option<&RssConfig<'_>>,
        queues: &mut Vec<Box<dyn Queue>>,
    ) -> anyhow::Result<()> {
        // Create the timers before the drivers move into the inner endpoint.
        let timers: Vec<_> = config
            .iter()
            .map(|c| PolledTimer::new(c.driver.as_ref()))
            .collect();
        let start = queues.len();
        self.inner.get_queues(config, rss, queues).await?;
        let inner_queues: Vec<_> = queues.drain(start..).collect();
        for (inner, timer) in inner_queues.into_iter().zip(timers) {
            queues.push(Box::new(RateLimitQueue {
                inner,
                shared: self.shared.clone(),
                timer,
                deadline: None,
                rx_lens: HashMap::new(),
            }));
        }
        Ok(())
    }

    async fn stop(&mut self) {
        self.inner.stop().await
    }

    fn is_ordered(&self) -> bool {
        self.inner.is_ordered()
    }

    fn tx_offload_support(&self) -> TxOffloadSupport {
        self.inner.tx_offload_support()
    }

    fn multiqueue_support(&self) -> MultiQueueSupport {
        self.inner.multiqueue_support()
    }

    fn tx_fast_completions(&self) -> bool {
        self.inner.tx_fast_completions()
    }

    async fn set_data_path_to_guest_vf(&self, use_vf: bool) -> anyhow::Result<()> {
        self.inner.set_data_path_to_guest_vf(use_vf).await
    }

    async fn get_data_path_to_guest_vf(&self) -> anyhow::Result<bool> {
        self.inner.get_data_path_to_guest_vf().await
    }

    async fn wait_for_endpoint_action(&mut self) -> EndpointAction {
        self.inner.wait_for_endpoint_action().await
    }

    fn link_speed(&self) -> u64 {
        self.inner.link_speed()
    }
}

/// Records the length of each received packet as the inner queue writes it,
/// since [`BufferAccess`] has no way to read it back.
struct Recording<'a> {
    inner: &'a mut dyn BufferAccess,
    lens: &'a mut HashMap<u32, usize>,
}

impl BufferAccess for Recording<'_> {
    fn guest_memory(&self) -> &GuestMemory {
        self.inner.guest_memory()
    }

    fn write_data(&mut self, id: RxId, data: &[u8]) {
        self.inner.write_data(id, data)
    }

    fn push_guest_addresses(&self, id: RxId, buf: &mut Vec<RxBufferSegment>) {
        self.inner.push_guest_addresses(id, buf)
    }

    fn capacity(&self, id: RxId) -> u32 {
        self.inner.capacity(id)
    }

    fn write_header(&mut self, id: RxId, metadata: &RxMetadata) {
        self.lens.insert(id.0, metadata.len);
        self.inner.write_header(id, metadata)
    }

    fn write_packet(&mut self, id: RxId, metadata: &RxMetadata, data: &[u8]) {
        self.lens.insert(id.0, metadata.len);
        self.inner.write_packet(id, metadata, data)
    }

    fn write_packet_segments(&mut self, id: RxId, metadata: &RxMetadata, segments: &[&[u8]]) {
        self.lens.insert(id.0, metadata.len);
        self.inner.write_packet_segments(id, metadata, segments)
    }
}

#[derive(InspectMut)]
struct RateLimitQueue {
    #[inspect(mut)]
    inner: Box<dyn Queue>,
    #[inspect(skip)]
    shared: Arc<Mutex<Shared>>,
    #[inspect(skip)]
    timer: PolledTimer,
    #[inspect(skip)]
    deadline: Option<pal_async::timer::Instant>,
    #[inspect(skip)]
    rx_lens: HashMap<u32, usize>,
}

impl RateLimitQueue {
    /// Schedules a wakeup after `wait`, keeping the earliest pending one.
    fn arm(&mut self, wait: Duration) {
        let deadline = pal_async::timer::Instant::now().saturating_add(wait);
        self.deadline = Some(match self.deadline {
            Some(d) if d <= deadline => d,
            _ => deadline,
        });
    }
}

#[async_trait]
impl Queue for RateLimitQueue {
    async fn update_target_vp(&mut self, target_vp: u32) {
        self.inner.update_target_vp(target_vp).await
    }

    fn poll_ready(&mut self, cx: &mut Context<'_>, pool: &mut dyn BufferAccess) -> Poll<()> {
        let mut rec = Recording {
            inner: pool,
            lens: &mut self.rx_lens,
        };
        if let Some(deadline) = self.deadline {
            if self.timer.poll_until(cx, deadline).is_ready() {
                self.deadline = None;
                return Poll::Ready(());
            }
            // Stay pending while throttled, but keep the inner waker
            // registered. The frontend re-polls everything after the timer.
            let _ = self.inner.poll_ready(cx, &mut rec);
            return Poll::Pending;
        }
        self.inner.poll_ready(cx, &mut rec)
    }

    fn rx_avail(&mut self, pool: &mut dyn BufferAccess, done: &[RxId]) {
        let mut rec = Recording {
            inner: pool,
            lens: &mut self.rx_lens,
        };
        self.inner.rx_avail(&mut rec, done)
    }

    fn rx_poll(
        &mut self,
        pool: &mut dyn BufferAccess,
        packets: &mut [RxId],
    ) -> anyhow::Result<usize> {
        let shared = self.shared.clone();
        let mut shared = shared.lock();
        if shared.rx.unlimited() {
            drop(shared);
            let n = self.inner.rx_poll(
                &mut Recording {
                    inner: pool,
                    lens: &mut self.rx_lens,
                },
                packets,
            )?;
            for id in &packets[..n] {
                self.rx_lens.remove(&id.0);
            }
            return Ok(n);
        }
        let now = Instant::now();
        let mut n = 0;
        while n < packets.len() {
            if let Some(wait) = shared.rx.ready(now) {
                self.arm(wait);
                break;
            }
            let got = self.inner.rx_poll(
                &mut Recording {
                    inner: pool,
                    lens: &mut self.rx_lens,
                },
                &mut packets[n..n + 1],
            )?;
            if got == 0 {
                break;
            }
            let len = self.rx_lens.remove(&packets[n].0).unwrap_or(0);
            shared.rx.consume(now, len as u64, 1);
            n += 1;
        }
        Ok(n)
    }

    fn tx_avail(
        &mut self,
        pool: &mut dyn BufferAccess,
        segments: &[TxSegment],
    ) -> anyhow::Result<(bool, usize)> {
        let shared = self.shared.clone();
        let mut admitted = 0;
        {
            let mut shared = shared.lock();
            if shared.tx.unlimited() {
                drop(shared);
                return self.inner.tx_avail(
                    &mut Recording {
                        inner: pool,
                        lens: &mut self.rx_lens,
                    },
                    segments,
                );
            }
            let now = Instant::now();
            let mut rest = segments;
            while !rest.is_empty() {
                if let Some(wait) = shared.tx.ready(now) {
                    self.arm(wait);
                    break;
                }
                let (meta, this, r) = next_packet(rest);
                shared.tx.consume(now, meta.len.into(), wire_packets(meta));
                admitted += this.len();
                rest = r;
            }
        }
        if admitted == 0 {
            return Ok((true, 0));
        }
        let (sync, sent) = self.inner.tx_avail(
            &mut Recording {
                inner: pool,
                lens: &mut self.rx_lens,
            },
            &segments[..admitted],
        )?;
        if sent < admitted {
            let mut shared = shared.lock();
            let mut rest = &segments[sent..admitted];
            while !rest.is_empty() {
                let (meta, _, r) = next_packet(rest);
                shared.tx.refund(meta.len.into(), wire_packets(meta));
                rest = r;
            }
        }
        Ok((sync, sent))
    }

    fn tx_poll(
        &mut self,
        pool: &mut dyn BufferAccess,
        done: &mut [TxId],
    ) -> Result<usize, TxError> {
        self.inner.tx_poll(
            &mut Recording {
                inner: pool,
                lens: &mut self.rx_lens,
            },
            done,
        )
    }

    fn queue_stats(&self) -> Option<&dyn BackendQueueStats> {
        self.inner.queue_stats()
    }
}

#[cfg(test)]
mod tests {
    use crate::NetRateLimits;
    use crate::RateLimitEndpoint;
    use async_trait::async_trait;
    use guestmem::GuestMemory;
    use inspect::InspectMut;
    use net_backend::BufferAccess;
    use net_backend::Endpoint;
    use net_backend::Queue;
    use net_backend::QueueConfig;
    use net_backend::RssConfig;
    use net_backend::RxId;
    use net_backend::RxMetadata;
    use net_backend::TxError;
    use net_backend::TxId;
    use net_backend::TxMetadata;
    use net_backend::TxSegment;
    use net_backend::TxSegmentType;
    use net_backend::loopback::LoopbackEndpoint;
    use net_backend::tests::Bufs;
    use pal_async::DefaultDriver;
    use pal_async::async_test;
    use std::collections::VecDeque;
    use std::future::poll_fn;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;
    use std::time::Instant;

    const TX_BASE: u64 = 512 * 1024;

    fn tx_packets(first_id: u32, n: u32, len: u32) -> Vec<TxSegment> {
        (first_id..first_id + n)
            .map(|i| TxSegment {
                ty: TxSegmentType::Head(TxMetadata {
                    id: TxId(i),
                    segment_count: 1,
                    len,
                    ..Default::default()
                }),
                gpa: TX_BASE + i as u64 * 2048,
                len,
            })
            .collect()
    }

    async fn one_queue(
        driver: &DefaultDriver,
        inner: Box<dyn Endpoint>,
        limits: NetRateLimits,
    ) -> (RateLimitEndpoint, Box<dyn Queue>) {
        let mut ep = RateLimitEndpoint::new(inner, limits);
        let mut queues = Vec::new();
        ep.get_queues(
            vec![QueueConfig {
                driver: Box::new(driver.clone()),
            }],
            None,
            &mut queues,
        )
        .await
        .unwrap();
        let q = queues.pop().unwrap();
        (ep, q)
    }

    fn rx_ids(range: std::ops::Range<u32>) -> Vec<RxId> {
        range.map(RxId).collect()
    }

    #[async_test]
    async fn tx_admits_burst_then_waits_for_refill(driver: DefaultDriver) {
        let mut pool = Bufs::new(GuestMemory::allocate(1024 * 1024));
        let (_ep, mut q) = one_queue(
            &driver,
            Box::new(LoopbackEndpoint::new()),
            NetRateLimits {
                bytes_per_sec: None,
                packets_per_sec: Some(100),
            },
        )
        .await;
        q.rx_avail(&mut pool, &rx_ids(0..64));

        // Burst is 10 packets; the debt model admits one more.
        let pkts = tx_packets(0, 30, 100);
        let (_, sent) = q.tx_avail(&mut pool, &pkts).unwrap();
        assert_eq!(sent, 11);
        let (_, sent) = q.tx_avail(&mut pool, &pkts[11..]).unwrap();
        assert_eq!(sent, 0);

        // The queue must wake when the debt is repaid (about 10ms).
        let start = Instant::now();
        poll_fn(|cx| q.poll_ready(cx, &mut pool)).await;
        assert!(
            start.elapsed() >= Duration::from_millis(5),
            "{:?}",
            start.elapsed()
        );
        let (_, sent) = q.tx_avail(&mut pool, &pkts[11..]).unwrap();
        assert!(sent >= 1);
    }

    #[async_test]
    async fn tx_refunds_packets_the_inner_queue_rejects(driver: DefaultDriver) {
        let mut pool = Bufs::new(GuestMemory::allocate(1024 * 1024));
        let (_ep, mut q) = one_queue(
            &driver,
            Box::new(LoopbackEndpoint::new()),
            NetRateLimits {
                bytes_per_sec: None,
                packets_per_sec: Some(100),
            },
        )
        .await;
        // Loopback only accepts as many packets as it has receive buffers.
        q.rx_avail(&mut pool, &rx_ids(0..5));
        let pkts = tx_packets(0, 30, 100);
        let (_, sent) = q.tx_avail(&mut pool, &pkts).unwrap();
        assert_eq!(sent, 5);
        // Five of ten tokens were used, so six more are admitted right away.
        q.rx_avail(&mut pool, &rx_ids(5..64));
        let (_, sent) = q.tx_avail(&mut pool, &pkts[5..]).unwrap();
        assert_eq!(sent, 6);
    }

    #[async_test]
    async fn no_limits_pass_everything(driver: DefaultDriver) {
        let mut pool = Bufs::new(GuestMemory::allocate(1024 * 1024));
        let (_ep, mut q) = one_queue(
            &driver,
            Box::new(LoopbackEndpoint::new()),
            NetRateLimits {
                bytes_per_sec: None,
                packets_per_sec: None,
            },
        )
        .await;
        q.rx_avail(&mut pool, &rx_ids(0..64));
        let (_, sent) = q.tx_avail(&mut pool, &tx_packets(0, 30, 100)).unwrap();
        assert_eq!(sent, 30);
        let mut out = [RxId(0); 64];
        assert_eq!(q.rx_poll(&mut pool, &mut out).unwrap(), 30);
    }

    /// An endpoint whose queue receives `len`-byte packets into every buffer
    /// it is given, without any transmits.
    #[derive(InspectMut)]
    #[inspect(skip)]
    struct RxSource {
        len: usize,
    }

    #[async_trait]
    impl Endpoint for RxSource {
        fn endpoint_type(&self) -> &'static str {
            "rx_source"
        }

        async fn get_queues(
            &mut self,
            config: Vec<QueueConfig>,
            _rss: Option<&RssConfig<'_>>,
            queues: &mut Vec<Box<dyn Queue>>,
        ) -> anyhow::Result<()> {
            for _ in config {
                queues.push(Box::new(RxSourceQueue {
                    len: self.len,
                    ready: VecDeque::new(),
                }));
            }
            Ok(())
        }

        async fn stop(&mut self) {}

        fn is_ordered(&self) -> bool {
            true
        }
    }

    #[derive(InspectMut)]
    #[inspect(skip)]
    struct RxSourceQueue {
        len: usize,
        ready: VecDeque<RxId>,
    }

    impl Queue for RxSourceQueue {
        fn poll_ready(&mut self, _cx: &mut Context<'_>, _pool: &mut dyn BufferAccess) -> Poll<()> {
            if self.ready.is_empty() {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }

        fn rx_avail(&mut self, pool: &mut dyn BufferAccess, done: &[RxId]) {
            for &id in done {
                pool.write_header(
                    id,
                    &RxMetadata {
                        offset: 0,
                        len: self.len,
                        ..Default::default()
                    },
                );
                self.ready.push_back(id);
            }
        }

        fn rx_poll(
            &mut self,
            _pool: &mut dyn BufferAccess,
            packets: &mut [RxId],
        ) -> anyhow::Result<usize> {
            let n = packets.len().min(self.ready.len());
            for (d, s) in packets.iter_mut().zip(self.ready.drain(..n)) {
                *d = s;
            }
            Ok(n)
        }

        fn tx_avail(
            &mut self,
            _pool: &mut dyn BufferAccess,
            segments: &[TxSegment],
        ) -> anyhow::Result<(bool, usize)> {
            Ok((true, segments.len()))
        }

        fn tx_poll(
            &mut self,
            _pool: &mut dyn BufferAccess,
            _done: &mut [TxId],
        ) -> Result<usize, TxError> {
            Ok(0)
        }
    }

    #[async_test]
    async fn tx_counts_segmentation_offload_as_wire_packets(driver: DefaultDriver) {
        let mut pool = Bufs::new(GuestMemory::allocate(1024 * 1024));
        let (_ep, mut q) = one_queue(
            &driver,
            Box::new(RxSource { len: 0 }),
            NetRateLimits {
                bytes_per_sec: None,
                packets_per_sec: Some(100),
            },
        )
        .await;
        // One TSO packet carrying 20 MSS-sized segments.
        let mss = 1448u16;
        let hdr = 14 + 20 + 20;
        let mut flags = net_backend::TxFlags::new();
        flags.set_offload_tcp_segmentation(true);
        let len = hdr as u32 + mss as u32 * 20;
        let mut pkts = tx_packets(0, 2, len);
        for p in &mut pkts {
            if let TxSegmentType::Head(meta) = &mut p.ty {
                meta.flags = flags;
                meta.l2_len = 14;
                meta.l3_len = 20;
                meta.l4_len = 20;
                meta.max_segment_size = mss;
            }
        }
        // The burst is 10 wire packets, so the first TSO packet is admitted
        // into 10 packets of debt and the second must wait.
        let (_, sent) = q.tx_avail(&mut pool, &pkts).unwrap();
        assert_eq!(sent, 1);
    }

    #[async_test]
    async fn rx_bytes_are_limited(driver: DefaultDriver) {
        let mut pool = Bufs::new(GuestMemory::allocate(1024 * 1024));
        // 100 KB/s with a 10 KB burst; 1 KB packets.
        let (_ep, mut q) = one_queue(
            &driver,
            Box::new(RxSource { len: 1000 }),
            NetRateLimits {
                bytes_per_sec: Some(100_000),
                packets_per_sec: None,
            },
        )
        .await;
        q.rx_avail(&mut pool, &rx_ids(0..100));
        let mut out = [RxId(0); 100];
        // Ten packets fill the burst and the debt model admits an eleventh.
        assert_eq!(q.rx_poll(&mut pool, &mut out).unwrap(), 11);
        assert_eq!(q.rx_poll(&mut pool, &mut out).unwrap(), 0);

        // Draining the remaining 89 packets takes about 0.89s.
        let start = Instant::now();
        let mut total = 11;
        while total < 100 {
            poll_fn(|cx| q.poll_ready(cx, &mut pool)).await;
            total += q.rx_poll(&mut pool, &mut out).unwrap();
        }
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(700), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    }
}
