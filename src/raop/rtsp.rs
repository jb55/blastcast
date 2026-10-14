//! RTSP as an AirPlay 1 sender speaks it to us: a request line, headers, a
//! blank line and a `Content-Length` body, answered in kind.

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// The longest header block we read: a sender's are a few hundred bytes.
const MAX_HEADER_BYTES: usize = 64 * 1024;
/// The largest body: cover art, which a phone has already scaled.
const MAX_BODY_BYTES: usize = 16 << 20;

#[derive(Debug, thiserror::Error)]
pub enum RtspError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("malformed request: {0}")]
    Malformed(&'static str),
    #[error("request too large")]
    TooLarge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    pub uri: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Request {
    /// The first header named `name`, in any case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Reads the next request, or `None` at a clean end of the stream.
    pub async fn read<R: AsyncBufRead + Unpin>(r: &mut R) -> Result<Option<Self>, RtspError> {
        let mut line = String::new();
        let mut read = 0;
        // Skip blank lines between requests.
        loop {
            line.clear();
            let n = r.read_line(&mut line).await?;
            if n == 0 {
                return Ok(None);
            }
            read += n;
            if !line.trim().is_empty() {
                break;
            }
            if read > MAX_HEADER_BYTES {
                return Err(RtspError::TooLarge);
            }
        }
        let mut parts = line.split_whitespace();
        let (Some(method), Some(uri)) = (parts.next(), parts.next()) else {
            return Err(RtspError::Malformed("request line"));
        };
        let (method, uri) = (method.to_owned(), uri.to_owned());

        let mut headers = Vec::new();
        loop {
            line.clear();
            let n = r.read_line(&mut line).await?;
            if n == 0 {
                return Err(RtspError::Malformed("headers end early"));
            }
            read += n;
            if read > MAX_HEADER_BYTES {
                return Err(RtspError::TooLarge);
            }
            let text = line.trim_end_matches(['\r', '\n']);
            if text.is_empty() {
                break;
            }
            let Some((k, v)) = text.split_once(':') else {
                return Err(RtspError::Malformed("header"));
            };
            headers.push((k.trim().to_owned(), v.trim().to_owned()));
        }

        let mut request = Self {
            method,
            uri,
            headers,
            body: Vec::new(),
        };
        let len: usize = match request.header("Content-Length") {
            Some(v) => v
                .parse()
                .map_err(|_| RtspError::Malformed("Content-Length"))?,
            None => 0,
        };
        if len > MAX_BODY_BYTES {
            return Err(RtspError::TooLarge);
        }
        request.body = vec![0; len];
        r.read_exact(&mut request.body).await?;
        Ok(Some(request))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn ok() -> Self {
        Self::new(200)
    }

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_owned(), value.into()));
        self
    }

    pub fn body(mut self, content_type: &str, body: Vec<u8>) -> Self {
        self.body = body;
        self.header("Content-Type", content_type)
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = format!("RTSP/1.0 {} {}\r\n", self.status, reason(self.status));
        for (k, v) in &self.headers {
            out.push_str(&format!("{k}: {v}\r\n"));
        }
        if !self.body.is_empty() {
            out.push_str(&format!("Content-Length: {}\r\n", self.body.len()));
        }
        out.push_str("\r\n");
        let mut bytes = out.into_bytes();
        bytes.extend_from_slice(&self.body);
        bytes
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        415 => "Unsupported Media Type",
        451 => "Parameter Not Understood",
        453 => "Not Enough Bandwidth",
        454 => "Session Not Found",
        455 => "Method Not Valid in This State",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        _ => "Unknown",
    }
}

/// The value of `key` in a `;`-separated header such as `Transport` or
/// `RTP-Info`: `control_port` in `RTP/AVP/UDP;control_port=6001`.
pub fn param<'a>(header: &'a str, key: &str) -> Option<&'a str> {
    header.split(';').find_map(|part| {
        let (k, v) = part.split_once('=')?;
        (k.trim() == key).then(|| v.trim())
    })
}

#[cfg(test)]
mod tests {
    use tokio::io::BufReader;

    use super::*;

    #[tokio::test]
    async fn reads_requests_back_to_back() {
        let wire = b"OPTIONS * RTSP/1.0\r\nCSeq: 1\r\nApple-Challenge: abc\r\n\r\n\
SET_PARAMETER rtsp://10.0.0.2/1 RTSP/1.0\r\nCSeq: 2\r\ncontent-length: 18\r\n\r\nvolume: -11.123456";
        let mut r = BufReader::new(&wire[..]);
        let a = Request::read(&mut r).await.unwrap().unwrap();
        assert_eq!((a.method.as_str(), a.uri.as_str()), ("OPTIONS", "*"));
        assert_eq!(a.header("cseq"), Some("1"));
        assert_eq!(a.header("Apple-Challenge"), Some("abc"));
        let b = Request::read(&mut r).await.unwrap().unwrap();
        assert_eq!(b.method, "SET_PARAMETER");
        assert_eq!(b.body, b"volume: -11.123456");
        assert_eq!(Request::read(&mut r).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_cut_off_request_is_an_error() {
        let mut r = BufReader::new(&b"OPTIONS * RTSP/1.0\r\nCSeq: 1\r\n"[..]);
        assert!(Request::read(&mut r).await.is_err());
    }

    #[test]
    fn writes_a_response() {
        let r = Response::ok()
            .header("CSeq", "3")
            .body("text/parameters", b"volume: -5.0\r\n".to_vec());
        assert_eq!(
            String::from_utf8(r.to_bytes()).unwrap(),
            "RTSP/1.0 200 OK\r\nCSeq: 3\r\nContent-Type: text/parameters\r\n\
Content-Length: 14\r\n\r\nvolume: -5.0\r\n"
        );
    }

    #[test]
    fn params_read_out_of_a_header() {
        let t =
            "RTP/AVP/UDP;unicast;interleaved=0-1;mode=record;control_port=6001;timing_port=6002";
        assert_eq!(param(t, "control_port"), Some("6001"));
        assert_eq!(param(t, "timing_port"), Some("6002"));
        assert_eq!(param(t, "server_port"), None);
        assert_eq!(param("seq=10;rtptime=4410", "rtptime"), Some("4410"));
    }
}
