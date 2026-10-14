//! A minimal RTSP/1.0 client: one request at a time on one TCP
//! connection, which is all a RAOP sender needs.
//!
//! Every request carries a `CSeq`, the headers iTunes sends (a
//! `User-Agent`, `Client-Instance`, `DACP-ID` and `Active-Remote`), and
//! once `SETUP` has answered, its `Session`.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// How long a receiver may take to answer one request.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// The largest response body read; RAOP's are a few bytes.
const MAX_BODY: usize = 64 * 1024;
const USER_AGENT: &str = "iTunes/7.6.2 (Windows; N;)";

#[derive(Debug, thiserror::Error)]
pub enum RtspError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{method} got no answer in {REQUEST_TIMEOUT:?}")]
    Timeout { method: String },
    #[error("{method} answered {status} {reason}")]
    Status {
        method: String,
        status: u16,
        reason: String,
    },
    #[error("malformed response: {0}")]
    Malformed(String),
}

/// A response's status, headers and body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub reason: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    /// The first header named `name`, ignoring case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Who we say we are, for each request's iTunes headers.
#[derive(Debug, Clone)]
pub struct Identity {
    /// 16 hex digits: `Client-Instance` and `DACP-ID`.
    pub instance: String,
    /// `Active-Remote`, a decimal u32.
    pub active_remote: u32,
}

pub struct RtspClient {
    stream: BufReader<TcpStream>,
    cseq: u32,
    identity: Identity,
    session: Option<String>,
}

impl RtspClient {
    pub fn new(stream: TcpStream, identity: Identity) -> Self {
        Self {
            stream: BufReader::new(stream),
            cseq: 0,
            identity,
            session: None,
        }
    }

    pub fn stream(&self) -> &TcpStream {
        self.stream.get_ref()
    }

    pub fn set_session(&mut self, session: String) {
        self.session = Some(session);
    }

    /// Reads whatever the receiver sends between requests, and drops it:
    /// 0 bytes once it has closed the connection. Cancel safe.
    pub async fn read_unsolicited(&mut self) -> std::io::Result<usize> {
        let n = self.stream.fill_buf().await?.len();
        self.stream.consume(n);
        Ok(n)
    }

    /// Sends one request and reads its response. A status other than 200
    /// is an error.
    pub async fn request(
        &mut self,
        method: &str,
        uri: &str,
        headers: &[(&str, String)],
        body: Option<(&str, &[u8])>,
    ) -> Result<Response, RtspError> {
        self.cseq += 1;
        let mut head = format!("{method} {uri} RTSP/1.0\r\nCSeq: {}\r\n", self.cseq);
        let mut add = |k: &str, v: &str| head.push_str(&format!("{k}: {v}\r\n"));
        for (k, v) in headers {
            add(k, v);
        }
        add("User-Agent", USER_AGENT);
        add("Client-Instance", &self.identity.instance);
        add("DACP-ID", &self.identity.instance);
        add("Active-Remote", &self.identity.active_remote.to_string());
        if let Some(session) = &self.session {
            add("Session", session);
        }
        if let Some((content_type, body)) = body {
            add("Content-Type", content_type);
            add("Content-Length", &body.len().to_string());
        }
        head.push_str("\r\n");
        tracing::trace!(method, uri, cseq = self.cseq, "rtsp request");
        let exchange = async {
            let mut bytes = head.into_bytes();
            if let Some((_, body)) = body {
                bytes.extend_from_slice(body);
            }
            self.stream.get_mut().write_all(&bytes).await?;
            read_response(&mut self.stream).await
        };
        let response =
            timeout(REQUEST_TIMEOUT, exchange)
                .await
                .map_err(|_| RtspError::Timeout {
                    method: method.to_owned(),
                })??;
        if response.status != 200 {
            return Err(RtspError::Status {
                method: method.to_owned(),
                status: response.status,
                reason: response.reason,
            });
        }
        Ok(response)
    }
}

async fn read_response<R: AsyncBufReadExt + Unpin>(r: &mut R) -> Result<Response, RtspError> {
    let mut line = String::new();
    if r.read_line(&mut line).await? == 0 {
        return Err(RtspError::Malformed("the connection closed".into()));
    }
    let mut parts = line.trim_end().splitn(3, ' ');
    let (Some(version), Some(status)) = (parts.next(), parts.next()) else {
        return Err(RtspError::Malformed(format!("status line {line:?}")));
    };
    if !version.starts_with("RTSP/") {
        return Err(RtspError::Malformed(format!("status line {line:?}")));
    }
    let status = status
        .parse()
        .map_err(|_| RtspError::Malformed(format!("status line {line:?}")))?;
    let reason = parts.next().unwrap_or("").to_owned();
    let mut headers = Vec::new();
    loop {
        line.clear();
        if r.read_line(&mut line).await? == 0 {
            return Err(RtspError::Malformed(
                "the connection closed in the headers".into(),
            ));
        }
        let header = line.trim_end();
        if header.is_empty() {
            break;
        }
        let Some((k, v)) = header.split_once(':') else {
            return Err(RtspError::Malformed(format!("header {header:?}")));
        };
        headers.push((k.trim().to_owned(), v.trim().to_owned()));
    }
    let length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .map(|(_, v)| v.parse::<usize>())
        .transpose()
        .map_err(|_| RtspError::Malformed("Content-Length".into()))?
        .unwrap_or(0);
    if length > MAX_BODY {
        return Err(RtspError::Malformed(format!("a {length}-byte body")));
    }
    let mut body = vec![0; length];
    r.read_exact(&mut body).await?;
    Ok(Response {
        status,
        reason,
        headers,
        body,
    })
}

/// The `key=value` parameters of a `Transport` header, e.g. `server_port`.
pub fn transport_param<'a>(transport: &'a str, key: &str) -> Option<&'a str> {
    transport
        .split(';')
        .filter_map(|p| p.split_once('='))
        .find(|(k, _)| k.trim() == key)
        .map(|(_, v)| v.trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_a_response_with_a_body() {
        let raw = b"RTSP/1.0 200 OK\r\nCSeq: 3\r\nContent-Length: 5\r\nAudio-Latency: 11025\r\n\r\nhelloRTSP/1.0 453 Not Enough Bandwidth\r\n\r\n";
        let mut r = BufReader::new(&raw[..]);
        let first = read_response(&mut r).await.unwrap();
        assert_eq!(first.status, 200);
        assert_eq!(first.header("audio-latency"), Some("11025"));
        assert_eq!(first.body, b"hello");
        let second = read_response(&mut r).await.unwrap();
        assert_eq!(
            (second.status, second.reason.as_str()),
            (453, "Not Enough Bandwidth")
        );
        assert!(read_response(&mut r).await.is_err());
    }

    #[test]
    fn transport_params() {
        let t =
            "RTP/AVP/UDP;unicast;mode=record;server_port=6000;control_port=6001;timing_port=6002";
        assert_eq!(transport_param(t, "server_port"), Some("6000"));
        assert_eq!(transport_param(t, "timing_port"), Some("6002"));
        assert_eq!(transport_param(t, "mode"), Some("record"));
        assert_eq!(transport_param(t, "nope"), None);
    }
}
