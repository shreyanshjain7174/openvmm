# gRPC / ttrpc

To enable a gRPC or ttrpc management interface, pass `--rpc`. This spawns an
OpenVMM process acting as an RPC server on the given Unix socket:

```bash
--rpc path=/path/to/openvmm.sock[,transport=<TRANSPORT>]
```

`transport` selects which wire protocol the server accepts:

* `auto` (default) — auto-detect ttrpc vs. gRPC per connection
* `ttrpc` — accept ttrpc clients only
* `grpc` — accept gRPC clients only

For example, to accept ttrpc clients only:

```bash
--rpc path=/path/to/openvmm.sock,transport=ttrpc
```

## VirtioFS configuration

`DevicesConfig.virtiofs_config` defines VirtioFS shares when the VM is
created. Each `VirtioFSConfig` supplies a tag and host root path. The
additive proto3 field `read_only` is field 3. Its absent or `false` value
keeps the default writable behavior. A value of `true` selects the
canonical `ro` host mount option, so the host backend enforces read-only
access.

```admonish warning title="Protocol compatibility"
A server generated from an older schema ignores the unknown `read_only`
field and therefore treats the share as writable. Keep the protocol
bindings and server version compatible when requesting read-only access.
```

Create-time VirtioFS configuration is distinct from adding or removing a
directory share while a VM is running. Supporting a share at VM creation
does not imply that a runtime `ModifyResource` path maps dynamic shares to
VirtioFS.

Here is a list of supported RPCs:

```admonish danger title="Disclaimer"
The following list is not exhaustive, and may be out of date. The most up to
date reference is the [`vmservice.proto`] file.

Moreover, many APIs defined in the `.proto` file may not be fully wired up yet.

In other words: This API is _very_ WIP, and user discretion is advised.
```

* CreateVM
* TeardownVM
* PauseVM
* ResumeVM
* WaitVM
* CapabilitiesVM
* PropertiesVM
* ModifyResource
* Quit

[`vmservice.proto`]: https://github.com/microsoft/openvmm/blob/main/openvmm/openvmm_ttrpc_vmservice/src/vmservice.proto
