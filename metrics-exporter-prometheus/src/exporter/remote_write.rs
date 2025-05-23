use std::time::Duration;

use http_body_util::{BodyExt, Collected, Full};
use hyper::{header::HeaderValue, Method, Request, Uri, body::Bytes};
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use tracing::{error, info};

use super::ExporterFuture;
use crate::PrometheusHandle;

/// Special label for the name of a metric.
pub const LABEL_NAME: &str = "__name__";
pub const CONTENT_TYPE: &str = "application/x-protobuf";
pub const HEADER_NAME_REMOTE_WRITE_VERSION: &str = "X-Prometheus-Remote-Write-Version";
pub const REMOTE_WRITE_VERSION_01: &str = "0.1.0";

#[derive(prost::Message, Clone, PartialEq)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

impl WriteRequest {
    /// Prepare the write request for sending.
    ///
    /// Ensures that the request conforms to the specification.
    /// See https://prometheus.io/docs/concepts/remote_write_spec.
    fn sort(&mut self) {
        for series in &mut self.timeseries {
            series.sort_labels_and_samples();
        }
    }

    fn sorted(mut self) -> Self {
        self.sort();
        self
    }

    /// Encode this write request as a protobuf message.
    pub fn encode_proto3(self) -> Vec<u8> {
        prost::Message::encode_to_vec(&self.sorted())
    }
    /// Encode this write request as a compressed protobuf message.
    /// NOTE: The API requires snappy compression, not a raw protobuf message.
    pub fn encode_compressed(self) -> Result<Vec<u8>, snap::Error> {
        snap::raw::Encoder::new().compress_vec(&self.encode_proto3())
    }
}

/// A time series.
///
/// .proto:
/// ```protobuf
/// message TimeSeries {
///   repeated Label labels   = 1;
///   repeated Sample samples = 2;
/// }
/// ```
#[derive(prost::Message, Clone, PartialEq)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

impl TimeSeries {
    /// Sort labels by name, and the samples by timestamp.
    ///
    /// Required by the specification.
    pub fn sort_labels_and_samples(&mut self) {
        self.labels.sort_by(|a, b| a.name.cmp(&b.name));
        self.samples.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    }
}

/// A label.
///
/// .proto:
/// ```protobuf
/// message Label {
///   string name  = 1;
///   string value = 2;
/// }
/// ```
#[derive(prost::Message, Clone, Hash, PartialEq, Eq)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A sample.
///
/// .proto:
/// ```protobuf
/// message Sample {
///   double value    = 1;
///   int64 timestamp = 2;
/// }
/// ```
#[derive(prost::Message, Clone, PartialEq)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

// Creates an ExporterFuture implementing remote write.
pub(super) fn new_remote_write(
    endpoint: Uri,
    interval: Duration,
    username: Option<String>,
    password: Option<String>,
    handle: PrometheusHandle,
) -> ExporterFuture {
    Box::pin(async move {
        let https = hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .expect("no native root CA certificates found")
            .https_or_http()
            .enable_http1()
            .build();
        let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .build(https);

        let auth = username.as_ref().map(|name| basic_auth(name, password.as_deref()));

        loop {
            // Sleep for `interval` amount of time, and then do a push.
            tokio::time::sleep(interval).await;

            let mut builder = Request::builder();
            if let Some(auth) = &auth {
                builder = builder.header("authorization", auth.clone());
            }

            let handle = handle.clone();
            let output = tokio::task::spawn_blocking(move || handle.remote_write()).await.unwrap();
            info!("E {:?}", output);
            let result = builder
                .method(Method::POST)
                .uri(endpoint.clone())
                .body(Full::new(output.encode_compressed().expect("TODO")
                .into())).expect("TODO");

            let resp = client.request(result).await;

            //TODO: Log errors from api
            error!("{:?}",String::from_utf8(resp.unwrap().into_body().collect().await.expect("d").to_bytes().into()).expect("d"));
        }
    })
}

#[cfg(feature = "push-gateway")]
fn basic_auth(username: &str, password: Option<&str>) -> HeaderValue {
    use base64::prelude::BASE64_STANDARD;
    use base64::write::EncoderWriter;
    use std::io::Write;

    let mut buf = b"Basic ".to_vec();
    {
        let mut encoder = EncoderWriter::new(&mut buf, &BASE64_STANDARD);
        write!(encoder, "{username}:").expect("should not fail to encode username");
        if let Some(password) = password {
            write!(encoder, "{password}").expect("should not fail to encode password");
        }
    }
    let mut header = HeaderValue::from_bytes(&buf).expect("base64 is always valid HeaderValue");
    header.set_sensitive(true);
    header
}

#[cfg(test)]
mod tests {
    use super::basic_auth;

    #[test]
    #[allow(clippy::similar_names)] // reader vs header, sheesh clippy
    pub fn test_basic_auth() {
        use base64::prelude::BASE64_STANDARD;
        use base64::read::DecoderReader;
        use std::io::Read;

        const BASIC: &str = "Basic ";

        // username only
        let username = "metrics";
        let header = basic_auth(username, None);

        let reader = &header.as_ref()[BASIC.len()..];
        let mut decoder = DecoderReader::new(reader, &BASE64_STANDARD);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result).unwrap();
        assert_eq!(b"metrics:", &result[..]);
        assert!(header.is_sensitive());

        // username/password
        let password = "123!_@ABC";
        let header = basic_auth(username, Some(password));

        let reader = &header.as_ref()[BASIC.len()..];
        let mut decoder = DecoderReader::new(reader, &BASE64_STANDARD);
        let mut result = Vec::new();
        decoder.read_to_end(&mut result).unwrap();
        assert_eq!(b"metrics:123!_@ABC", &result[..]);
        assert!(header.is_sensitive());
    }
}
