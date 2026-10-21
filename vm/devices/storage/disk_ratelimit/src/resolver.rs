// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Resolver for [`RateLimitDiskHandle`].

use crate::RateLimitDisk;
use crate::RateLimits;
use async_trait::async_trait;
use disk_backend::resolve::ResolveDiskParameters;
use disk_backend::resolve::ResolvedDisk;
use disk_backend_resources::RateLimitDiskHandle;
use vm_resource::AsyncResolveResource;
use vm_resource::ResourceResolver;
use vm_resource::declare_static_async_resolver;
use vm_resource::kind::DiskHandleKind;

/// A resolver for [`RateLimitDisk`].
pub struct RateLimitDiskResolver;
declare_static_async_resolver!(RateLimitDiskResolver, (DiskHandleKind, RateLimitDiskHandle));

#[async_trait]
impl AsyncResolveResource<DiskHandleKind, RateLimitDiskHandle> for RateLimitDiskResolver {
    type Output = ResolvedDisk;
    type Error = anyhow::Error;

    async fn resolve(
        &self,
        resolver: &ResourceResolver,
        rsrc: RateLimitDiskHandle,
        input: ResolveDiskParameters<'_>,
    ) -> Result<Self::Output, Self::Error> {
        let inner = resolver.resolve(rsrc.disk, input).await?;
        let limits = RateLimits {
            bytes_per_sec: rsrc.bytes_per_sec,
            ops_per_sec: rsrc.ops_per_sec,
        };
        ResolvedDisk::new(RateLimitDisk::new(limits, inner.0, input.driver_source))
            .map_err(|e| anyhow::anyhow!("failed to create the rate-limited disk: {}", e))
    }
}
