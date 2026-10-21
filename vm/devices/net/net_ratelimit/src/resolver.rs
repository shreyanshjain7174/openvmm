// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Resolver for [`RateLimitHandle`].

use crate::NetRateLimits;
use crate::RateLimitEndpoint;
use async_trait::async_trait;
use net_backend::resolve::ResolveEndpointParams;
use net_backend::resolve::ResolvedEndpoint;
use net_backend_resources::ratelimit::RateLimitHandle;
use vm_resource::AsyncResolveResource;
use vm_resource::CanResolveTo;
use vm_resource::ResourceResolver;
use vm_resource::declare_static_async_resolver;
use vm_resource::kind::NetEndpointHandleKind;

/// A resolver for [`RateLimitEndpoint`].
pub struct RateLimitResolver;
declare_static_async_resolver!(RateLimitResolver, (NetEndpointHandleKind, RateLimitHandle));

#[async_trait]
impl AsyncResolveResource<NetEndpointHandleKind, RateLimitHandle> for RateLimitResolver {
    type Output = ResolvedEndpoint;
    type Error = anyhow::Error;

    async fn resolve(
        &self,
        resolver: &ResourceResolver,
        rsrc: RateLimitHandle,
        input: <NetEndpointHandleKind as CanResolveTo<ResolvedEndpoint>>::Input<'_>,
    ) -> Result<Self::Output, Self::Error> {
        let inner: ResolvedEndpoint = resolver
            .resolve(
                rsrc.endpoint,
                ResolveEndpointParams {
                    mac_address: input.mac_address,
                },
            )
            .await?;
        Ok(RateLimitEndpoint::new(
            inner.0,
            NetRateLimits {
                bytes_per_sec: rsrc.bytes_per_sec,
                packets_per_sec: rsrc.packets_per_sec,
            },
        )
        .into())
    }
}
