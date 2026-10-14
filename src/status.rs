//! What a client has told the server about itself: its hello's player
//! support, merged with every `client/state` since.
//!
//! A [`Connection`](crate::Connection) holds one [`PlayerStatus`] in a
//! watch channel. The server mirrors volume, mute and availability into the
//! player registry; the streamer reads the timing fields to pick its
//! send-ahead and subscribes to see them change.

use crate::protocol::{
    ClientHello, ClientState, MAX_OUTPUT_DELAY_MS, PlayerCommandKind, PlayerStateReport,
};

/// A client's last reported state. Every `client/state` is merged in: a
/// field the update leaves out keeps its value, as the older clients that
/// send only what changed need.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlayerStatus {
    /// Whether the client can take part in playback. False until its first
    /// `client/state`.
    pub available: bool,
    /// 0-100, once the player has reported it.
    pub volume: Option<u8>,
    pub muted: Option<bool>,
    pub timing: PlayerTiming,
    /// The commands from the hello's support object (`volume`, `mute`).
    pub hello_commands: Vec<PlayerCommandKind>,
    /// The commands from the latest `client/state` that listed any
    /// (`set_output_delay`); each list replaces the one before.
    pub state_commands: Vec<PlayerCommandKind>,
}

/// What the streamer schedules a player's audio by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PlayerTiming {
    /// Delay after the audio port (external speakers, an amplifier),
    /// 0-5000. The player subtracts it from chunk timestamps, so the
    /// server sends that much further ahead.
    pub output_delay_ms: u32,
    /// Startup lead the player asks for, from the stream/start or
    /// stream/clear that starts playback to its first chunk. A hint.
    pub required_lead_time_ms: Option<u32>,
    /// Audio the player wants queued at all times during playback.
    pub min_buffer_ms: Option<u32>,
    /// Bytes of encoded audio the player can hold, from its hello. A hard
    /// cap; 0 when the hello didn't say.
    pub buffer_capacity: u64,
}

/// What one `client/state` changed, beyond the status itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatusChange {
    /// This was the client's first `client/state`.
    pub first: bool,
    /// The client went from available to unavailable: its output was taken
    /// by something else, and it leaves its group.
    pub became_unavailable: bool,
    /// The status is different from before.
    pub changed: bool,
}

impl PlayerStatus {
    /// The status of a client that has said hello and nothing since.
    pub fn from_hello(hello: &ClientHello) -> Self {
        let support = hello.player();
        Self {
            hello_commands: support
                .map(|s| s.supported_commands.clone())
                .unwrap_or_default(),
            timing: PlayerTiming {
                buffer_capacity: support.map_or(0, |s| s.buffer_capacity),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Whether the player said it accepts `command`, in its hello or its
    /// latest state. The spec has players ignore any other.
    pub fn supports(&self, command: PlayerCommandKind) -> bool {
        self.hello_commands.contains(&command) || self.state_commands.contains(&command)
    }

    /// Merges one `client/state` in. The first one marks the client
    /// available even when it says nothing about availability: an older
    /// client's state only ever carries volume, and it is ready once it
    /// sends one.
    pub fn apply(&mut self, state: &ClientState, first: bool) -> StatusChange {
        let before = self.clone();
        match state.available() {
            Some(available) => self.available = available,
            None if first => self.available = true,
            None => {}
        }
        if let Some(player) = &state.player {
            self.apply_player(player);
        }
        StatusChange {
            first,
            became_unavailable: before.available && !self.available,
            changed: *self != before,
        }
    }

    fn apply_player(&mut self, p: &PlayerStateReport) {
        if let Some(volume) = p.volume {
            self.volume = Some(volume.min(100) as u8);
        }
        if let Some(muted) = p.muted {
            self.muted = Some(muted);
        }
        if let Some(delay) = p.output_delay_ms() {
            self.timing.output_delay_ms = delay.min(MAX_OUTPUT_DELAY_MS);
        }
        if let Some(lead) = p.required_lead_time_ms {
            self.timing.required_lead_time_ms = Some(lead);
        }
        if let Some(min) = p.min_buffer_ms {
            self.timing.min_buffer_ms = Some(min);
        }
        if let Some(commands) = &p.supported_commands {
            self.state_commands = commands.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ClientMessage, PlayerSupport};

    fn state(payload: &str) -> ClientState {
        let text = format!(r#"{{"type":"client/state","payload":{payload}}}"#);
        let ClientMessage::State(s) = ClientMessage::decode(&text).unwrap() else {
            panic!("not a client/state: {payload}")
        };
        s
    }

    fn hello() -> ClientHello {
        ClientHello {
            client_id: "c".into(),
            name: "C".into(),
            player_v1_support: Some(PlayerSupport {
                buffer_capacity: 1_000_000,
                supported_commands: vec![PlayerCommandKind::Volume, PlayerCommandKind::Mute],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn starts_from_the_hello() {
        let s = PlayerStatus::from_hello(&hello());
        assert!(!s.available);
        assert_eq!(s.timing.buffer_capacity, 1_000_000);
        assert!(s.supports(PlayerCommandKind::Volume));
        assert!(s.supports(PlayerCommandKind::Mute));
        assert!(!s.supports(PlayerCommandKind::SetOutputDelay));
        assert_eq!(s.volume, None);
    }

    #[test]
    fn maps_the_spec_player_object() {
        let mut s = PlayerStatus::from_hello(&hello());
        let change = s.apply(
            &state(
                r#"{"available":true,"player":{"volume":40,"muted":true,"output_delay_ms":120,
                "required_lead_time_ms":300,"min_buffer_ms":80,"supported_commands":["set_output_delay"]}}"#,
            ),
            true,
        );
        assert!(change.first && change.changed && !change.became_unavailable);
        assert!(s.available);
        assert_eq!((s.volume, s.muted), (Some(40), Some(true)));
        assert_eq!(
            s.timing,
            PlayerTiming {
                output_delay_ms: 120,
                required_lead_time_ms: Some(300),
                min_buffer_ms: Some(80),
                buffer_capacity: 1_000_000,
            }
        );
        assert!(s.supports(PlayerCommandKind::SetOutputDelay));
        assert!(s.supports(PlayerCommandKind::Volume), "hello's still count");
    }

    #[test]
    fn reads_aiosendspin_static_delay_and_clamps() {
        let mut s = PlayerStatus::from_hello(&hello());
        s.apply(
            &state(
                r#"{"available":true,"player":{"volume":250,"static_delay_ms":9000,"supported_commands":["set_static_delay"]}}"#,
            ),
            true,
        );
        assert_eq!(s.volume, Some(100));
        assert_eq!(s.timing.output_delay_ms, MAX_OUTPUT_DELAY_MS);
        assert!(s.supports(PlayerCommandKind::SetStaticDelay));
    }

    #[test]
    fn an_update_keeps_what_it_leaves_out() {
        let mut s = PlayerStatus::from_hello(&hello());
        s.apply(
            &state(r#"{"player":{"state":"synchronized","volume":80,"muted":false}}"#),
            true,
        );
        let change = s.apply(&state(r#"{"player":{"volume":81}}"#), false);
        assert!(change.changed);
        assert!(s.available);
        assert_eq!((s.volume, s.muted), (Some(81), Some(false)));
        let change = s.apply(&state(r#"{"player":{"volume":81}}"#), false);
        assert!(!change.changed, "a repeat changes nothing");
    }

    #[test]
    fn the_first_state_makes_a_silent_client_available() {
        let mut s = PlayerStatus::from_hello(&hello());
        s.apply(&state(r#"{"player":{"volume":5}}"#), true);
        assert!(s.available);

        let mut s = PlayerStatus::from_hello(&hello());
        let change = s.apply(&state(r#"{"available":false}"#), true);
        assert!(!s.available);
        assert!(!change.became_unavailable, "it never was available");
    }

    #[test]
    fn notices_only_the_falling_edge() {
        let mut s = PlayerStatus::from_hello(&hello());
        s.apply(&state(r#"{"available":true}"#), true);
        assert!(
            s.apply(&state(r#"{"state":"external_source"}"#), false)
                .became_unavailable
        );
        assert!(
            !s.apply(&state(r#"{"available":false}"#), false)
                .became_unavailable
        );
        let back = s.apply(&state(r#"{"available":true}"#), false);
        assert!(back.changed && !back.became_unavailable && s.available);
    }
}
