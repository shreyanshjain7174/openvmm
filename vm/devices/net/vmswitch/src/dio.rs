// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! This module implements a `Pollable` interface to vmswitch's DirectIO NIC
//! type. This provides a tap-like interface to vmswitch on Windows, allowing
//! Ethernet frames to be sent and received.

use super::kernel::SwitchPortId;
use super::kernel::c16;
use super::vmsif;
use futures::AsyncRead;
use guid::Guid;
use pal::windows::Overlapped;
use pal::windows::SendSyncRawHandle;
use pal::windows::status_to_error;
use pal_async::driver::Driver;
use pal_async::wait::PolledWait;
use pal_event::Event;
use std::io;
use std::io::ErrorKind;
use std::io::Write;
use std::os::windows::prelude::*;
use std::pin::Pin;
use std::ptr;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use windows_sys::Win32::Foundation::ERROR_NOT_FOUND;
use windows_sys::Win32::Foundation::STATUS_SUCCESS;
use windows_sys::Win32::Storage::FileSystem::ReadFile;
use windows_sys::Win32::Storage::FileSystem::WriteFile;
use windows_sys::Win32::System::IO::CancelIoEx;
use windows_sys::Win32::System::Threading::INFINITE;
use windows_sys::Win32::System::Threading::WaitForSingleObject;
use zerocopy::FromBytes;
use zerocopy::Immutable;
use zerocopy::IntoBytes;
use zerocopy::KnownLayout;

const MINIMUM_FRAME_SIZE: usize = 60;
pub const FRAME_SIZE: usize = 1514;
const OUT_OP_COUNT: usize = 32;
const IN_OP_COUNT: usize = 2;
const IN_BUFFER_SIZE: usize = 32765;

pub struct DioNic {
    f: OwnedHandle,
    nic_name: String,
}

pub struct DioQueue {
    state: QueueState, // must be first so that it's dropped before the nic
    nic: DioNic,
}

struct QueueState {
    handle: SendSyncRawHandle,
    in_next_full: (usize, usize),
    in_next_pending: usize,
    in_buf: Box<[[u8; IN_BUFFER_SIZE]; IN_OP_COUNT]>,
    in_event: PolledWait<Event>,
    in_overlapped: Box<[Overlapped; IN_OP_COUNT]>,
    out_buf: Box<[[u8; FRAME_SIZE]; OUT_OP_COUNT]>,
    out_overlapped: Box<[Overlapped; OUT_OP_COUNT]>,
}

#[repr(C)]
#[derive(IntoBytes, Immutable, KnownLayout, FromBytes)]
struct DioNicPacketHeader {
    len: u32,
    next: u32,
}

impl DioNic {
    /// Creates a new direct IO NIC, not connected to any switch.
    pub fn new(
        vm_id: Guid,
        nic_name: &str,
        friendly_name: &str,
        mac_address: [u8; 6],
    ) -> io::Result<Self> {
        let full_nic_name = format!("{}--{}", vm_id, nic_name);
        let path = format!(r#"\\.\VmSwitch\{}"#, full_nic_name);

        let handle = unsafe {
            let mut raw_handle = ptr::null_mut();
            vmsif::chk(vmsif::VmsIfNicCreateEmulated(
                &mut raw_handle,
                c16(path)?.as_ptr(),
            ))?;
            let handle = OwnedHandle::from_raw_handle(raw_handle);
            let vm_id_16 = c16(vm_id.to_string())?;
            vmsif::chk(vmsif::VmsIfNicMorphToEmulatedNic(
                handle.as_raw_handle(),
                c16(&full_nic_name)?.as_ptr(),
                c16(friendly_name)?.as_ptr(),
                c16(Guid::new_random().to_string())?.as_ptr(),
                vm_id_16.as_ptr(),
                vm_id_16.as_ptr(),
                &mac_address,
                true,
                0,
                0x100,
            ))?;

            handle
        };

        Ok(Self {
            f: handle,
            nic_name: full_nic_name,
        })
    }

    /// Connects the NIC to a port on the given switch.
    pub fn connect(&mut self, id: &SwitchPortId) -> io::Result<()> {
        let (switch16, port16) = id.c_ids();
        unsafe {
            vmsif::chk(vmsif::VmsIfNicConnect(
                self.f.as_raw_handle(),
                switch16.as_ptr(),
                port16.as_ptr(),
                c16(&self.nic_name)?.as_ptr(),
                Duration::from_secs(10).as_millis() as u32,
            ))?;

            Ok(())
        }
    }
}

impl DioQueue {
    pub fn new(driver: &(impl ?Sized + Driver), nic: DioNic) -> Self {
        // All read operations use the same event. This can cause spurious
        // wakeups (rarely, since reads should generally be completed by
        // vmswitch in order), but it cannot cause missed wakeups since we never
        // wait on the event and issue a new IO using the event concurrently.
        let in_event = PolledWait::new(driver, Event::new()).unwrap();
        let in_overlapped: Box<_> = (0..IN_OP_COUNT)
            .map(|_| {
                let mut o = Overlapped::new();
                o.set_event(in_event.get().as_handle().as_raw_handle());
                o
            })
            .collect();
        // Write operations do not use an event since we only need to wait for a
        // write to finish in `drop`, where spurious wakeups from completing
        // reads will not be a significant issue.
        let out_overlapped = Default::default();
        let handle = nic.f.as_raw_handle();
        let mut this = Self {
            nic,
            state: QueueState {
                handle: SendSyncRawHandle(handle),
                in_next_full: (0, 0),
                in_next_pending: 0,
                in_buf: Box::new([[0; IN_BUFFER_SIZE]; IN_OP_COUNT]),
                in_event,
                in_overlapped: in_overlapped.try_into().ok().unwrap(),
                out_buf: Box::new([[0; FRAME_SIZE]; OUT_OP_COUNT]),
                out_overlapped,
            },
        };
        for i in 0..IN_OP_COUNT {
            this.start_read(i)
        }
        this
    }

    pub fn into_inner(self) -> DioNic {
        let Self { state, nic } = self;
        // Ensure all IOs are cancelled.
        drop(state);
        nic
    }

    /// Checks if there are incoming packets ready to be processed. Fails with
    /// `ErrorKind::WouldBlock` if there are no packets ready.
    fn process_in(&mut self) -> io::Result<()> {
        if self.state.in_next_full.0 != self.state.in_next_pending {
            Ok(())
        } else {
            match self.state.in_overlapped[self.state.in_next_pending].io_status() {
                Some((STATUS_SUCCESS, _)) => {
                    self.state.in_next_pending = (self.state.in_next_pending + 1) % IN_OP_COUNT;
                    Ok(())
                }
                None => Err(ErrorKind::WouldBlock.into()),
                Some((status, _)) => Err(status_to_error(status)),
            }
        }
    }

    /// Initiates a read to vmswitch.
    fn start_read(&mut self, buf_index: usize) {
        unsafe {
            let buf = &mut self.state.in_buf[buf_index];
            ReadFile(
                self.nic.f.as_raw_handle(),
                buf.as_mut_ptr().cast(),
                buf.len() as u32,
                ptr::null_mut(),
                self.state.in_overlapped[buf_index].as_ptr(),
            );
        }
    }

    pub fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            if self.state.in_next_full.0 != self.state.in_next_pending
                || self.state.in_overlapped[self.state.in_next_pending]
                    .io_status()
                    .is_some()
            {
                break Poll::Ready(());
            }
            std::task::ready!(self.state.in_event.poll_wait(cx))
                .expect("wait on handle cannot fail");
        }
    }

    pub fn read_with<F, R>(&mut self, f: F) -> io::Result<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        self.process_in()?;

        let (buf_index, offset) = self.state.in_next_full;
        let buf = &self.state.in_buf[buf_index][offset..];
        let (header, data) = DioNicPacketHeader::read_from_prefix(buf).unwrap(); // TODO: zerocopy: unwrap (https://github.com/microsoft/openvmm/issues/759)
        let len = header.len as usize;
        let r = f(&data[..len]);
        if header.next != 0 {
            self.state.in_next_full = (buf_index, offset + header.next as usize);
        } else {
            // This batch of packets is done, so the buffer is available again.
            // Start the next read operation.
            self.start_read(buf_index);
            self.state.in_next_full = ((buf_index + 1) % IN_OP_COUNT, 0);
        }
        Ok(r)
    }

    pub fn write_with<F, R>(&mut self, mut len: usize, f: F) -> Option<R>
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        for (i, o) in self.state.out_overlapped.iter_mut().enumerate() {
            if let Some((status, _)) = o.io_status() {
                // This overlapped is available for reuse.
                if status != STATUS_SUCCESS {
                    tracing::warn!(
                        error = &status_to_error(status) as &dyn std::error::Error,
                        "packet write failure"
                    );
                }
                let buf = &mut self.state.out_buf[i];
                let r = f(&mut buf[..len]);
                // Zero pad short frames out to the minimum.
                if len < MINIMUM_FRAME_SIZE {
                    for b in &mut buf[len..MINIMUM_FRAME_SIZE] {
                        *b = 0;
                    }
                    len = MINIMUM_FRAME_SIZE;
                }
                unsafe {
                    WriteFile(
                        self.nic.f.as_raw_handle(),
                        buf.as_ptr().cast(),
                        len as u32,
                        ptr::null_mut(),
                        o.as_ptr(),
                    );
                }
                return Some(r);
            }
        }
        tracing::warn!("dropped packet");
        None
    }
}

impl AsyncRead for DioQueue {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        loop {
            std::task::ready!(this.poll_read_ready(cx));
            let res = this.read_with(|data| {
                buf[..data.len()].copy_from_slice(data);
                data.len()
            });
            match res {
                Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                r => break Poll::Ready(r),
            }
        }
    }
}

impl Write for DioQueue {
    fn write(&mut self, packet: &[u8]) -> io::Result<usize> {
        self.write_with(packet.len(), |buf| buf.copy_from_slice(packet));
        Ok(packet.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for QueueState {
    fn drop(&mut self) {
        // Cancel and wait on any outstanding IO to release the overlapped
        // structures and buffers. Reads may have been issued by another
        // thread, so cancel IO from all threads.
        unsafe {
            // ERROR_NOT_FOUND just means nothing was pending.
            if CancelIoEx(self.handle.0, ptr::null()) == 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() != Some(ERROR_NOT_FOUND as i32) {
                    tracing::warn!(
                        error = &err as &dyn std::error::Error,
                        "failed to cancel DIO IO"
                    );
                }
            }
        }
        for o in self.in_overlapped.iter() {
            while o.io_status().is_none() {
                // BUGBUG: it's possible that the event signal will be lost
                // since it's associated with an IO driver...
                self.in_event.get().wait();
            }
        }
        for o in self.out_overlapped.iter() {
            while o.io_status().is_none() {
                unsafe {
                    // Writes are started without an event but will signal the
                    // file object on completion.
                    WaitForSingleObject(self.handle.0, INFINITE);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DioNic;
    use super::DioQueue;
    use super::FRAME_SIZE;
    use crate::kernel::SwitchPort;
    use crate::kernel::SwitchPortId;
    use futures::AsyncReadExt;
    use futures::FutureExt;
    use guid::Guid;
    use pal_async::DefaultDriver;
    use pal_async::async_test;
    use pal_async::driver::Driver;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use std::time::Instant;

    /// A random locally administered unicast MAC, so tests never collide.
    fn random_mac() -> [u8; 6] {
        let mut mac = [0; 6];
        getrandom::fill(&mut mac).unwrap();
        mac[0] = (mac[0] & 0xfe) | 0x02;
        mac
    }

    fn connected_nic(driver: &impl Driver) -> (DioQueue, SwitchPort) {
        connected_nic_with_mac(driver, random_mac())
    }

    fn connected_nic_with_mac(driver: &impl Driver, mac: [u8; 6]) -> (DioQueue, SwitchPort) {
        let vm_id = Guid::new_random();
        let mut e = DioNic::new(vm_id, "nic", "my nic", mac).unwrap();
        // Connect to the Default Switch by well-known GUID.
        let id = SwitchPortId {
            switch: crate::hcn::DEFAULT_SWITCH,
            port: Guid::new_random(),
        };
        let port = SwitchPort::new(&id).unwrap();
        e.connect(&id).unwrap();
        let queue = DioQueue::new(driver, e);
        (queue, port)
    }

    #[async_test]
    #[ignore] // Requires vmswitch and admin privileges
    async fn test_default_switch(driver: DefaultDriver) {
        let (mut e, _port) = connected_nic(&driver);
        let mut packet = [0; FRAME_SIZE];
        assert!(e.read(&mut packet).now_or_never().is_none());
    }

    /// Runs `f` on a new thread and returns its result, or `None` if it did not
    /// finish within `limit` (the thread is leaked in that case).
    fn run_bounded<R: Send + 'static>(
        limit: Duration,
        f: impl FnOnce() -> R + Send + 'static,
    ) -> Option<R> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(limit).ok()
    }

    const DROP_LIMIT: Duration = Duration::from_secs(5);

    /// Dropping a queue on the thread that created it cancels the initial
    /// reads promptly.
    #[async_test]
    #[ignore] // Requires vmswitch and admin privileges
    async fn drop_on_creating_thread_is_prompt(driver: DefaultDriver) {
        let elapsed = run_bounded(DROP_LIMIT, move || {
            let (queue, _port) = connected_nic(&driver);
            let start = Instant::now();
            drop(queue);
            start.elapsed()
        })
        .expect("drop on the creating thread did not complete");
        assert!(elapsed < Duration::from_secs(1), "took {elapsed:?}");
    }

    /// Dropping a queue on a thread other than the one that created it must
    /// still cancel the outstanding reads.
    #[async_test]
    #[ignore] // Requires vmswitch and admin privileges
    async fn drop_on_other_thread_is_prompt(driver: DefaultDriver) {
        let (queue, _port) = connected_nic(&driver);
        run_bounded(DROP_LIMIT, move || drop(queue))
            .expect("drop on a different thread did not complete: reads were not cancelled");
    }

    /// A read re-issued after a frame is consumed on one thread must be
    /// cancelled when the queue is dropped on another.
    #[async_test]
    #[ignore] // Requires vmswitch and admin privileges
    async fn drop_after_frame_consumed_on_other_thread_is_prompt(driver: DefaultDriver) {
        let sender_mac = random_mac();
        let (mut sender, _sender_port) = connected_nic_with_mac(&driver, sender_mac);
        // 0 = waiting for frame, 1 = frame consumed, 2 = drop started.
        let stage = Arc::new(AtomicU32::new(0));
        let stage2 = stage.clone();
        let result = run_bounded(DROP_LIMIT * 3, move || {
            let (queue, _port) = connected_nic(&driver);
            sender.write_with(60, |buf| {
                buf[..6].fill(0xff);
                buf[6..12].copy_from_slice(&sender_mac);
                buf[12..14].copy_from_slice(&[0x88, 0xb5]);
                buf[14..].fill(0);
            });
            // Keep the consumer alive: Windows cancels a thread's pending IO when it exits.
            let stage3 = stage2.clone();
            let (queue_tx, queue_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let consumer = std::thread::spawn(move || {
                let mut queue = queue;
                let deadline = Instant::now() + DROP_LIMIT;
                // Poll directly; the async test thread is blocked, so no event loop runs.
                loop {
                    match queue.read_with(|_| ()) {
                        Ok(()) => break,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(e) => panic!("no frame consumed: {e}"),
                    }
                }
                stage2.store(1, Ordering::SeqCst);
                queue_tx.send(queue).unwrap();
                let _ = release_rx.recv();
            });
            let queue = queue_rx
                .recv()
                .expect("consumer failed before consuming a frame");
            let start = Instant::now();
            stage3.store(2, Ordering::SeqCst);
            drop(queue);
            let elapsed = start.elapsed();
            drop(release_tx);
            consumer.join().unwrap();
            (elapsed, sender)
        });
        let (elapsed, _sender) = result.unwrap_or_else(|| {
            panic!(
                "timed out at stage {} (2 = the drop hung)",
                stage.load(Ordering::SeqCst)
            )
        });
        assert!(elapsed < Duration::from_secs(1), "drop took {elapsed:?}");
    }
}
