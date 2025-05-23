#[cfg(feature = "http-listener")]
use http_listener::HttpListeningError;
#[cfg(any(feature = "http-listener", feature = "push-gateway"))]
use std::future::Future;
#[cfg(feature = "http-listener")]
use std::net::SocketAddr;
#[cfg(any(feature = "http-listener", feature = "push-gateway"))]
use std::pin::Pin;
#[cfg(feature = "push-gateway")]
use std::time::Duration;

#[cfg(feature = "push-gateway")]
use hyper::Uri;

/// Error types possible from an exporter
#[cfg(any(feature = "http-listener", feature = "push-gateway"))]
#[derive(Debug)]
pub enum ExporterError {
    #[cfg(feature = "http-listener")]
    HttpListener(HttpListeningError),
    PushGateway(()),
}
/// Convenience type for Future implementing an exporter.
#[cfg(any(feature = "http-listener", feature = "push-gateway", feature = "remote-write"))]
pub type ExporterFuture = Pin<Box<dyn Future<Output = Result<(), ExporterError>> + Send + 'static>>;

#[cfg(feature = "http-listener")]
#[derive(Clone, Debug)]
enum ListenDestination {
    Tcp(SocketAddr),
    #[cfg(feature = "uds-listener")]
    Uds(std::path::PathBuf),
}

#[derive(Clone, Debug)]
enum ExporterConfig {
    // Run an HTTP listener on the given `listen_address`.
    #[cfg(feature = "http-listener")]
    HttpListener { destination: ListenDestination },

    // Run a push gateway task sending to the given `endpoint` after `interval` time has elapsed,
    // infinitely.
    #[cfg(feature = "push-gateway")]
    PushGateway {
        endpoint: Uri,
        interval: Duration,
        username: Option<String>,
        password: Option<String>,
        use_http_post_method: bool,
    },

    // Run a remote write task sending to the given `endpoint` after `interval` time has elapsed,
    // infinitely.
    #[cfg(feature = "remote-write")]
    RemoteWrite {
        endpoint: Uri,
        interval: Duration,
        username: Option<String>,
        password: Option<String>,
    },

    #[allow(dead_code)]
    Unconfigured,
}

impl ExporterConfig {
    #[cfg_attr(not(any(feature = "http-listener", feature = "push-gateway", feature = "remote-write")), allow(dead_code))]
    fn as_type_str(&self) -> &'static str {
        match self {
            #[cfg(feature = "http-listener")]
            Self::HttpListener { .. } => "http-listener",
            #[cfg(feature = "push-gateway")]
            Self::PushGateway { .. } => "push-gateway",
            #[cfg(feature = "remote-write")]
            Self::RemoteWrite { .. } => "remote-write",
            Self::Unconfigured => "unconfigured,",
        }
    }
}

#[cfg(feature = "http-listener")]
mod http_listener;

#[cfg(feature = "push-gateway")]
mod push_gateway;

#[cfg(feature = "remote-write")]
pub mod remote_write;

pub(crate) mod builder;
