use hickory_resolver::{TokioAsyncResolver, system_conf};
use reqwest::dns::{Addrs, Name, Resolving, Resolve};
use std::sync::Arc;
use std::net::SocketAddr;
use once_cell::sync::OnceCell;

/// A hickory resolver that uses extended DNS (eDNS) to resolve domain names. We use this to
/// circumvent a bug in the hickory resolver: hickory allocates a buffer of 512 bytes for
/// name server replies, but we observed >512 bytes replies in Azure. Enabling eDNS
/// circumvents this problem because hickory determines the receive buffer size differently
/// with eDNS. Unfortunately, we have not figured out an easier way to enable eDNS than
/// implementing a custom resolver. Our implementation is based on reqwest's hickory
/// wrapper, see https://github.com/Xuanwo/reqwest-hickory-resolver/blob/main/src/lib.rs.
#[derive(Debug, Default, Clone)]
pub(super) struct HickoryResolverWithEdns {
    // Delay construction as initialization might be outside the Tokio runtime context.
    state: Arc<OnceCell<TokioAsyncResolver>>,
}

impl Resolve for HickoryResolverWithEdns {
    fn resolve(&self, name: Name) -> Resolving {
        let hickory_resolver = self.clone();
        Box::pin(async move {
            let resolver = hickory_resolver.state.get_or_try_init(new_resolver)?;

            let lookup = resolver.lookup_ip(name.as_str()).await?;

            let addrs: Addrs = Box::new(lookup.into_iter().map(|addr| SocketAddr::new(addr, 0)));
            Ok(addrs)
        })
    }
}

/// Create a new resolver with the default configuration,
/// which reads from `/etc/resolve.conf`.
fn new_resolver() -> std::io::Result<TokioAsyncResolver> {
    let (config, mut opts) = system_conf::read_system_conf().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("error reading DNS system conf: {}", e),
        )
    })?;

    opts.edns0 = true;

    Ok(TokioAsyncResolver::tokio(config, opts))
}