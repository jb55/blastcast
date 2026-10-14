//! The controller, metadata and artwork roles over real websockets: a
//! controller sets its group's volume and mute, and a display gets the
//! group's metadata and its artwork scaled to the channel it asked for.

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use image::{ImageFormat, Rgb, RgbImage};
use tas_core::PlayerRegistry;
use tas_sendspin::protocol::{
    ArtworkChannelSupport, ArtworkSupport, BinaryFrame, ClientCommand, ClientHello, ClientMessage,
    ClientState, ControllerCommand, ControllerState, HeaderLayout, MediaCommand, PlayerCommandKind,
    PlayerStateReport, PlayerSupport, Progress, RequestFormatArtwork, ServerMessage,
    StreamRequestFormat, binary,
};
use tas_sendspin::{SendspinServer, ServerConfig, TrackMetadata};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // a test channel, not a hot path
enum Got {
    Text(ServerMessage),
    Binary(Vec<u8>),
}

struct Client {
    tx: mpsc::UnboundedSender<ClientMessage>,
    rx: mpsc::UnboundedReceiver<Got>,
}

impl Client {
    /// Connects, says hello and sends `state`.
    async fn connect(url: &str, hello: ClientHello, state: ClientState) -> Self {
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let (mut sink, mut stream) = ws.split();
        let (tx, mut out) = mpsc::unbounded_channel::<ClientMessage>();
        tokio::spawn(async move {
            while let Some(msg) = out.recv().await {
                if sink.send(Message::text(msg.to_json())).await.is_err() {
                    break;
                }
            }
        });
        let (got, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(Ok(msg)) = stream.next().await {
                let msg = match msg {
                    Message::Text(text) => Got::Text(ServerMessage::decode(&text).unwrap()),
                    Message::Binary(frame) => Got::Binary(frame.to_vec()),
                    _ => continue,
                };
                if got.send(msg).is_err() {
                    break;
                }
            }
        });
        let client = Self { tx, rx };
        client.send(ClientMessage::Hello(Box::new(hello)));
        client.send(ClientMessage::State(state));
        client
    }

    fn send(&self, msg: ClientMessage) {
        self.tx.send(msg).unwrap();
    }

    fn command(&self, command: ControllerCommand) {
        self.send(ClientMessage::Command(ClientCommand {
            controller: Some(command),
        }));
    }

    /// Skips messages until `f` picks one.
    async fn expect<T>(&mut self, what: &str, mut f: impl FnMut(&Got) -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let got = tokio::time::timeout_at(deadline, self.rx.recv())
                .await
                .unwrap_or_else(|_| panic!("never got {what}"))
                .expect("connection closed");
            if let Some(t) = f(&got) {
                return t;
            }
        }
    }

    async fn controller(&mut self, what: &str, f: impl Fn(&ControllerState) -> bool) {
        self.expect(what, |g| match g {
            Got::Text(ServerMessage::State(s)) => s
                .controller
                .as_ref()
                .and_then(|c| c.as_ref())
                .filter(|c| f(c))
                .map(drop),
            _ => None,
        })
        .await
    }

    /// The next `server/command` to the player, as (kind, volume, mute).
    async fn player_command(&mut self) -> (PlayerCommandKind, Option<u32>, Option<bool>) {
        self.expect("server/command", |g| match g {
            Got::Text(ServerMessage::Command(c)) => {
                let p = c.player.unwrap();
                Some((p.command, p.volume, p.mute))
            }
            _ => None,
        })
        .await
    }

    /// The next artwork image on `channel`, decoded; `None` for a clear.
    async fn artwork(&mut self, channel: u8) -> Option<(u32, u32)> {
        self.expect("artwork", |g| match g {
            Got::Binary(frame) => {
                let f = BinaryFrame::decode(frame, HeaderLayout::Legacy).unwrap();
                assert_ne!(f.kind, binary::ARTWORK_CHANNEL_0 + 1, "channel 1 is none");
                (f.artwork_channel() == Some(channel)).then(|| {
                    (!f.payload.is_empty()).then(|| {
                        let img = image::load_from_memory(f.payload).unwrap();
                        (img.width(), img.height())
                    })
                })
            }
            _ => None,
        })
        .await
    }
}

async fn start() -> Arc<SendspinServer> {
    SendspinServer::new(
        ServerConfig::new("srv", "Test"),
        Arc::new(PlayerRegistry::default()),
    )
}

async fn serve(server: &Arc<SendspinServer>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/sendspin", listener.local_addr().unwrap());
    tokio::spawn(server.clone().serve(listener));
    url
}

fn speaker(id: &str) -> ClientHello {
    ClientHello {
        client_id: id.into(),
        name: id.into(),
        version: 1,
        supported_roles: vec!["player@v1".into()],
        player_v1_support: Some(PlayerSupport {
            supported_commands: vec![PlayerCommandKind::Volume, PlayerCommandKind::Mute],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn at_volume(volume: u32) -> ClientState {
    ClientState {
        available: Some(true),
        player: Some(PlayerStateReport {
            volume: Some(volume),
            muted: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn available() -> ClientState {
    ClientState {
        available: Some(true),
        ..Default::default()
    }
}

fn command(command: MediaCommand) -> ControllerCommand {
    ControllerCommand {
        command,
        volume: None,
        mute: None,
        position_ms: None,
        offset_ms: None,
    }
}

async fn until_connected(server: &SendspinServer, ids: &[&str]) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !ids.iter().all(|id| server.groups().group_of(id).is_some()) {
        assert!(tokio::time::Instant::now() < deadline, "never connected");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn png(w: u32, h: u32) -> Vec<u8> {
    let mut out = Vec::new();
    RgbImage::from_pixel(w, h, Rgb([200, 30, 30]))
        .write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
        .unwrap();
    out
}

#[tokio::test]
async fn a_controller_sets_its_groups_volume_and_mute() {
    let server = start().await;
    let url = serve(&server).await;
    let mut kitchen = Client::connect(&url, speaker("kitchen"), at_volume(20)).await;
    let mut den = Client::connect(&url, speaker("den"), at_volume(60)).await;
    let remote_hello = ClientHello {
        client_id: "remote".into(),
        name: "Remote".into(),
        version: 1,
        supported_roles: vec!["controller@v1".into()],
        ..Default::default()
    };
    let mut remote = Client::connect(&url, remote_hello, available()).await;
    until_connected(&server, &["kitchen", "den", "remote"]).await;

    remote
        .controller("a lone controller's state", |c| {
            c.supported_commands == [MediaCommand::Switch] && c.volume == 100
        })
        .await;

    let groups = server.groups();
    let group = groups.group_of("kitchen").unwrap().id;
    groups.join("den", &group).await.unwrap();
    groups.join("remote", &group).await.unwrap();
    remote
        .controller("the group's average volume", |c| {
            c.volume == 40
                && !c.muted
                && c.supported_commands
                    == [
                        MediaCommand::Volume,
                        MediaCommand::Mute,
                        MediaCommand::Switch,
                    ]
        })
        .await;

    remote.command(ControllerCommand {
        volume: Some(50),
        ..command(MediaCommand::Volume)
    });
    assert_eq!(
        kitchen.player_command().await,
        (PlayerCommandKind::Volume, Some(30), None)
    );
    assert_eq!(
        den.player_command().await,
        (PlayerCommandKind::Volume, Some(70), None),
        "relative levels kept"
    );

    remote.command(ControllerCommand {
        mute: Some(true),
        ..command(MediaCommand::Mute)
    });
    assert_eq!(
        kitchen.player_command().await,
        (PlayerCommandKind::Mute, None, Some(true))
    );
    assert_eq!(
        den.player_command().await,
        (PlayerCommandKind::Mute, None, Some(true))
    );

    // The players' own reports are what the controller is shown.
    kitchen.send(ClientMessage::State(at_volume(30)));
    den.send(ClientMessage::State(at_volume(70)));
    remote
        .controller("the volume the players report", |c| c.volume == 50)
        .await;

    // Nothing plays, so there is no stop to offer, and it is ignored.
    remote.command(command(MediaCommand::Stop));
    remote.command(ControllerCommand {
        volume: Some(0),
        ..command(MediaCommand::Volume)
    });
    assert_eq!(
        kitchen.player_command().await,
        (PlayerCommandKind::Volume, Some(0), None),
        "the stop before it sent nothing"
    );
}

#[tokio::test]
async fn a_display_gets_its_groups_metadata_and_artwork() {
    let server = start().await;
    let url = serve(&server).await;
    let panel_hello = ClientHello {
        client_id: "panel".into(),
        name: "Panel".into(),
        version: 1,
        supported_roles: vec!["metadata@v1".into(), "artwork@v1".into()],
        artwork_v1_support: Some(ArtworkSupport {
            channels: vec![
                ArtworkChannelSupport {
                    source: "album".into(),
                    format: "png".into(),
                    media_width: 64,
                    media_height: 32,
                },
                ArtworkChannelSupport {
                    source: "none".into(),
                    format: "jpeg".into(),
                    media_width: 10,
                    media_height: 10,
                },
            ],
        }),
        ..Default::default()
    };
    let mut panel = Client::connect(&url, panel_hello, available()).await;
    until_connected(&server, &["panel"]).await;
    let group = server.groups().group_of("panel").unwrap().id;

    let channels = panel
        .expect("artwork stream/start", |g| match g {
            Got::Text(ServerMessage::StreamStart(s)) => s.artwork.clone(),
            _ => None,
        })
        .await
        .channels;
    assert_eq!(
        channels
            .iter()
            .map(|c| (c.source.as_str(), c.width, c.height))
            .collect::<Vec<_>>(),
        [("album", 64, 32), ("none", 10, 10)]
    );
    assert_eq!(panel.artwork(0).await, None, "nothing to show yet");

    let progress = Progress {
        track_progress: 1_000,
        track_duration: 180_000,
        playback_speed: 1000,
    };
    server
        .groups()
        .set_metadata(
            &group,
            Some(TrackMetadata {
                title: Some("Song".into()),
                artist: Some("Band".into()),
                progress: Some(progress),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let meta = panel
        .expect("metadata", |g| match g {
            Got::Text(ServerMessage::State(s)) => s.metadata.clone(),
            _ => None,
        })
        .await
        .expect("set");
    assert_eq!(meta.title, Some(Some("Song".into())));
    assert_eq!(meta.album, Some(None), "full state: null, not omitted");
    assert_eq!(meta.progress, Some(Some(progress)));

    server
        .groups()
        .set_artwork(&group, Some(png(300, 300).into()))
        .await
        .unwrap();
    assert_eq!(panel.artwork(0).await, Some((64, 32)));

    panel.send(ClientMessage::RequestFormat(StreamRequestFormat {
        artwork: Some(RequestFormatArtwork {
            channel: 0,
            media_width: Some(16),
            media_height: Some(16),
            ..Default::default()
        }),
        ..Default::default()
    }));
    let resized = panel
        .expect("a new artwork stream/start", |g| match g {
            Got::Text(ServerMessage::StreamStart(s)) => s.artwork.clone(),
            _ => None,
        })
        .await;
    assert_eq!(
        (resized.channels[0].width, resized.channels[0].height),
        (16, 16)
    );
    assert_eq!(panel.artwork(0).await, Some((16, 16)), "the image again");

    server.groups().set_artwork(&group, None).await.unwrap();
    assert_eq!(panel.artwork(0).await, None, "cleared");
    server.groups().set_metadata(&group, None).await.unwrap();
    panel
        .expect("metadata: null", |g| match g {
            Got::Text(ServerMessage::State(s)) => (s.metadata == Some(None)).then_some(()),
            _ => None,
        })
        .await;
}
