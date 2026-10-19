// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Defines the resource resolver for virtiofs devices.

use crate::VirtioFs;
use crate::virtio::VirtioFsDevice;
use futures::StreamExt;
use lxutil::LxVolumeOptions;
use pal_async::task::Spawn;
use virtio::resolve::ResolvedVirtioDevice;
use virtio::resolve::VirtioResolveInput;
use virtio_resources::fs::VirtioFsAggregateChild;
use virtio_resources::fs::VirtioFsBackend;
use virtio_resources::fs::VirtioFsHandle;
use virtio_resources::fs::VirtioFsShareRequest;
use vm_resource::ResolveResource;
use vm_resource::declare_static_resolver;
use vm_resource::kind::VirtioDeviceHandle;

/// Resolver for virtiofs devices.
pub struct VirtioFsResolver;

declare_static_resolver! {
    VirtioFsResolver,
    (VirtioDeviceHandle, VirtioFsHandle),
}

impl ResolveResource<VirtioDeviceHandle, VirtioFsHandle> for VirtioFsResolver {
    type Output = ResolvedVirtioDevice;
    type Error = anyhow::Error;

    fn resolve(
        &self,
        resource: VirtioFsHandle,
        input: VirtioResolveInput<'_>,
    ) -> Result<Self::Output, Self::Error> {
        let device = match resource.fs {
            VirtioFsBackend::HostFs {
                root_path,
                mount_options,
            } => VirtioFsDevice::new(
                input.driver_source,
                &resource.tag,
                VirtioFs::new(
                    &root_path,
                    Some(&LxVolumeOptions::from_option_string(&mount_options)),
                )?,
                0,
                None,
            ),
            #[cfg(windows)]
            VirtioFsBackend::SectionFs { root_path } => {
                VirtioFsDevice::new(
                    input.driver_source,
                    &resource.tag,
                    crate::SectionFs::new(&root_path)?,
                    8 * 1024 * 1024 * 1024, // 8GB of shared memory,
                    None,
                )
            }
            #[cfg(not(windows))]
            VirtioFsBackend::SectionFs { .. } => {
                anyhow::bail!("section fs not supported on this platform")
            }
            VirtioFsBackend::Aggregate { children, requests } => {
                let fs = VirtioFs::new_aggregate();
                for child in children {
                    add_aggregate_child(&fs, child)?;
                }
                if let Some(requests) = requests {
                    input
                        .driver_source
                        .simple()
                        .spawn(
                            "virtiofs-shares",
                            handle_share_requests(fs.clone(), requests),
                        )
                        .detach();
                }
                VirtioFsDevice::new(input.driver_source, &resource.tag, fs, 0, None)
            }
        };
        Ok(device.into())
    }
}

fn add_aggregate_child(fs: &VirtioFs, child: VirtioFsAggregateChild) -> anyhow::Result<()> {
    fs.add_child_restricted(
        &child.name,
        &child.root_path,
        Some(&LxVolumeOptions::from_option_string(&child.mount_options)),
        child.allowed_names,
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "failed to add virtiofs aggregate child '{}' (root_path='{}'): {e}",
            child.name,
            child.root_path
        )
    })
}

/// Serves runtime add and remove requests for an aggregate device.
pub(crate) async fn handle_share_requests(
    fs: VirtioFs,
    mut requests: mesh::Receiver<VirtioFsShareRequest>,
) {
    while let Some(req) = requests.next().await {
        match req {
            VirtioFsShareRequest::AddChild(rpc) => {
                rpc.handle_failable_sync(|child| add_aggregate_child(&fs, child))
            }
            VirtioFsShareRequest::RemoveChild(rpc) => rpc.handle_failable_sync(|name| {
                fs.remove_child(&name)
                    .map_err(|e| anyhow::anyhow!("failed to remove virtiofs child '{name}': {e}"))
            }),
        }
    }
}
