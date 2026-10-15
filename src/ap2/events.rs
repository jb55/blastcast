//! The event channel: the TCP connection a sender opens to SETUP's
//! `eventPort`, encrypted from its first byte with the pairing's `Events`
//! keys ([`Keys::events`]).
//!
//! It is the receiver's way back to the sender (remote commands, an
//! `updateInfo`), and we send nothing on it yet. A sender that leaves it
//! unopened gives up on the session, so it is accepted and kept open, and
//! anything the sender says on it is answered 200 and logged.

use std::time::Duration;

use tokio::io::BufReader;
use tokio::net::TcpListener;

use super::Keys;
use super::channel::{Role, SecureRead, SecureWrite};
use crate::raop::rtsp::{Request, Response};

/// How long the sender has to open it.
const OPEN_WITHIN: Duration = Duration::from_secs(30);

/// Takes the first connection to `listener` and serves it until it
/// closes.
pub async fn serve(listener: TcpListener, keys: Keys) {
    let (tcp, from) = match tokio::time::timeout(OPEN_WITHIN, listener.accept()).await {
        Ok(Ok(accepted)) => accepted,
        Ok(Err(e)) => {
            tracing::debug!(error = %e, "accepting the AirPlay 2 event channel");
            return;
        }
        Err(_) => {
            tracing::debug!("the AirPlay 2 sender never opened its event channel");
            return;
        }
    };
    drop(listener);
    tracing::debug!(%from, "AirPlay 2 event channel open");
    let _ = tcp.set_nodelay(true);
    let (read, write) = tcp.into_split();
    let (sealer, opener) = keys.events(Role::Accessory);
    let mut read = SecureRead::new(read);
    read.encrypt(opener, Vec::new());
    let mut read = BufReader::new(read);
    let mut write = SecureWrite::new(write);
    write.encrypt(sealer);
    loop {
        let request = match Request::read(&mut read).await {
            Ok(Some(r)) => r,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(error = %e, "AirPlay 2 event channel");
                break;
            }
        };
        tracing::debug!(method = %request.method, uri = %request.uri, "AirPlay 2 event");
        let mut response = Response::ok();
        if let Some(cseq) = request.header("CSeq") {
            response = response.header("CSeq", cseq);
        }
        if write.send(&response.to_bytes()).await.is_err() {
            break;
        }
    }
    tracing::debug!("AirPlay 2 event channel closed");
}
