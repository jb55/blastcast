//! Sendspin groups: every client is in exactly one, and a group plays one
//! [`AudioStream`] to all its players, so they share one timeline and one
//! send-ahead, the largest any member needs.
//!
//! A client gets a group of its own when it connects, named after it, and
//! `group/update` with the group's full state: `playback_state`,
//! `group_id` and `group_name`. Every change it sees later is sent the same
//! way, in full.
//!
//! - **Joining** a group moves a client out of its old one. Its old stream
//!   sends it `stream/end`, and only once that is queued does the new
//!   stream send `stream/start`, followed by the chunks still ahead that
//!   the rest of the group has (see the stream's late joiners).
//! - **Leaving** moves a client to a new group of its own, stopped, after
//!   `stream/end`. A group that loses its last member is dropped.
//! - **Unavailable** (`available: false`): a client in a group with others
//!   is moved to a stopped group of its own, and its old group remembered
//!   as its previous one for `switch`. Alone, its group stops. It is never
//!   moved back when it returns.
//! - **Volume and mute** go to every member that takes them, all set to the
//!   same value. How a group's volume maps to its members' is the caller's
//!   choice; this layer only carries it out.
//!
//! The groups are run by one task, so a change is applied whole before the
//! next starts; queries read a snapshot it publishes after each one.

use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, RandomState};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tas_audio::{AudioError, LiveSource, ServerClock};
use tokio::sync::{mpsc, oneshot};

use crate::connection::Connection;
use crate::protocol::{GroupUpdate, PlaybackState, PlayerCommand, ServerCommand, ServerMessage};
use crate::stream::AudioStream;

/// A group as it is now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupInfo {
    pub id: String,
    pub name: String,
    /// Client ids, in the order they joined.
    pub members: Vec<String>,
    pub playback_state: PlaybackState,
}

#[derive(Debug, thiserror::Error)]
pub enum GroupError {
    #[error("no group {0}")]
    NoGroup(String),
    #[error("no connected client {0}")]
    NoClient(String),
    #[error("group {0} is not playing")]
    NotPlaying(String),
    #[error("can't stream the source: {0}")]
    Audio(#[from] AudioError),
    #[error("the groups task has stopped")]
    Stopped,
}

/// The groups of one server. Clones share them.
#[derive(Clone)]
pub struct Groups {
    ops: mpsc::UnboundedSender<Op>,
    view: Arc<Mutex<View>>,
}

impl std::fmt::Debug for Groups {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Groups")
            .field("groups", &self.list())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct View {
    groups: BTreeMap<String, GroupInfo>,
    member_of: HashMap<String, String>,
    previous: HashMap<String, String>,
}

type Reply<T> = oneshot::Sender<Result<T, GroupError>>;

enum Op {
    Connect(Arc<Connection>),
    Disconnect(Arc<Connection>),
    Unavailable(Arc<Connection>),
    Join {
        client_id: String,
        group_id: String,
        reply: Reply<()>,
    },
    Leave {
        client_id: String,
        reply: Reply<String>,
    },
    Rename {
        group_id: String,
        name: String,
        reply: Reply<()>,
    },
    Play {
        group_id: String,
        source: Box<dyn LiveSource>,
        reply: Reply<()>,
    },
    QueueNext {
        group_id: String,
        source: Box<dyn LiveSource>,
        reply: Reply<()>,
    },
    Stop {
        group_id: String,
        reply: Reply<()>,
    },
    Volume {
        group_id: String,
        volume: u32,
        reply: Reply<()>,
    },
    Mute {
        group_id: String,
        mute: bool,
        reply: Reply<()>,
    },
    /// A group's stream played out; `generation` names which one.
    Ended {
        group_id: String,
        generation: u64,
    },
}

impl Groups {
    /// Starts the task that runs the groups; call it inside a Tokio
    /// runtime. Streams are timed on `clock`, the one players sync to.
    pub fn new(clock: ServerClock) -> Self {
        let (ops, rx) = mpsc::unbounded_channel();
        let view = Arc::new(Mutex::new(View::default()));
        let actor = Actor {
            clock,
            ops: ops.downgrade(),
            view: view.clone(),
            groups: BTreeMap::new(),
            member_of: HashMap::new(),
            previous: HashMap::new(),
            id_prefix: format!("{:08x}", RandomState::new().hash_one(0u8) as u32),
            next_id: 0,
            next_generation: 0,
        };
        tokio::spawn(actor.run(rx));
        Self { ops, view }
    }

    /// Every group, by id.
    pub fn list(&self) -> Vec<GroupInfo> {
        self.view().groups.values().cloned().collect()
    }

    pub fn get(&self, group_id: &str) -> Option<GroupInfo> {
        self.view().groups.get(group_id).cloned()
    }

    /// The group `client_id` is in, if it is connected.
    pub fn group_of(&self, client_id: &str) -> Option<GroupInfo> {
        let view = self.view();
        let group_id = view.member_of.get(client_id)?;
        view.groups.get(group_id).cloned()
    }

    /// The group a client was in before going unavailable moved it out,
    /// which `switch` goes back to first. Cleared when it joins or leaves
    /// a group, or disconnects.
    pub fn previous_group(&self, client_id: &str) -> Option<String> {
        self.view().previous.get(client_id).cloned()
    }

    /// Moves `client_id` into `group_id`. A playing group starts streaming
    /// to it straight away, with the audio the others are about to play.
    pub async fn join(&self, client_id: &str, group_id: &str) -> Result<(), GroupError> {
        self.call(|reply| Op::Join {
            client_id: client_id.into(),
            group_id: group_id.into(),
            reply,
        })
        .await
    }

    /// Moves `client_id` to a stopped group of its own and returns that
    /// group's id. A client already alone stays where it is.
    pub async fn leave(&self, client_id: &str) -> Result<String, GroupError> {
        self.call(|reply| Op::Leave {
            client_id: client_id.into(),
            reply,
        })
        .await
    }

    pub async fn rename(&self, group_id: &str, name: &str) -> Result<(), GroupError> {
        self.call(|reply| Op::Rename {
            group_id: group_id.into(),
            name: name.into(),
            reply,
        })
        .await
    }

    /// Plays `source` to the group. A group already playing jumps to it,
    /// as [`AudioStream::seek`] does.
    pub async fn play(
        &self,
        group_id: &str,
        source: Box<dyn LiveSource>,
    ) -> Result<(), GroupError> {
        self.call(|reply| Op::Play {
            group_id: group_id.into(),
            source,
            reply,
        })
        .await
    }

    /// Plays `source` gaplessly after what the group is playing.
    pub async fn queue_next(
        &self,
        group_id: &str,
        source: Box<dyn LiveSource>,
    ) -> Result<(), GroupError> {
        self.call(|reply| Op::QueueNext {
            group_id: group_id.into(),
            source,
            reply,
        })
        .await
    }

    /// Stops the group's stream: its players get `stream/end`.
    pub async fn stop(&self, group_id: &str) -> Result<(), GroupError> {
        self.call(|reply| Op::Stop {
            group_id: group_id.into(),
            reply,
        })
        .await
    }

    /// Sets every member that takes a volume command to `volume`, 0-100.
    pub async fn set_volume(&self, group_id: &str, volume: u32) -> Result<(), GroupError> {
        self.call(|reply| Op::Volume {
            group_id: group_id.into(),
            volume,
            reply,
        })
        .await
    }

    /// Mutes or unmutes every member that takes a mute command.
    pub async fn set_mute(&self, group_id: &str, mute: bool) -> Result<(), GroupError> {
        self.call(|reply| Op::Mute {
            group_id: group_id.into(),
            mute,
            reply,
        })
        .await
    }

    /// A client connected: it gets a group of its own, or, replacing an
    /// older connection of the same client, takes its place in its group.
    pub(crate) fn connect(&self, conn: Arc<Connection>) {
        let _ = self.ops.send(Op::Connect(conn));
    }

    /// A connection ended. Ignored if a newer one has taken its place.
    pub(crate) fn disconnect(&self, conn: Arc<Connection>) {
        let _ = self.ops.send(Op::Disconnect(conn));
    }

    /// A client reported `available: false`.
    pub(crate) fn unavailable(&self, conn: Arc<Connection>) {
        let _ = self.ops.send(Op::Unavailable(conn));
    }

    async fn call<T>(&self, op: impl FnOnce(Reply<T>) -> Op) -> Result<T, GroupError> {
        let (reply, rx) = oneshot::channel();
        self.ops.send(op(reply)).map_err(|_| GroupError::Stopped)?;
        rx.await.map_err(|_| GroupError::Stopped)?
    }

    fn view(&self) -> MutexGuard<'_, View> {
        self.view.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct Group {
    id: String,
    name: String,
    members: Vec<Arc<Connection>>,
    stream: Option<Playing>,
}

struct Playing {
    stream: AudioStream,
    /// Tells this stream's end apart from an earlier one's.
    generation: u64,
}

impl Group {
    fn playback_state(&self) -> PlaybackState {
        if self.stream.is_some() {
            PlaybackState::Playing
        } else {
            PlaybackState::Stopped
        }
    }

    fn info(&self) -> GroupInfo {
        GroupInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            members: self
                .members
                .iter()
                .map(|m| m.client_id().to_owned())
                .collect(),
            playback_state: self.playback_state(),
        }
    }

    /// Sends `conn` the group's full state.
    fn update(&self, conn: &Connection) {
        let msg = ServerMessage::GroupUpdate(GroupUpdate {
            playback_state: Some(self.playback_state()),
            group_id: Some(self.id.clone()),
            group_name: Some(self.name.clone()),
        });
        if let Err(e) = conn.send(msg) {
            tracing::debug!(client_id = conn.client_id(), error = %e, "group/update not sent");
        }
    }

    fn update_all(&self) {
        for conn in &self.members {
            self.update(conn);
        }
    }

    fn is_member(&self, conn: &Arc<Connection>) -> bool {
        self.members.iter().any(|m| Arc::ptr_eq(m, conn))
    }

    /// Sends `command` to every member whose player listed it.
    fn command(&self, command: PlayerCommand) {
        for conn in &self.members {
            if !conn.has_role("player") || !conn.player_status().supports(command.command) {
                continue;
            }
            let msg = ServerMessage::Command(ServerCommand {
                player: Some(command),
            });
            if let Err(e) = conn.send(msg) {
                tracing::debug!(client_id = conn.client_id(), error = %e, "command not sent");
            }
        }
    }
}

struct Actor {
    clock: ServerClock,
    /// For a stream's end to come back as an [`Op`]; weak, so the task
    /// stops once every [`Groups`] is dropped.
    ops: mpsc::WeakUnboundedSender<Op>,
    view: Arc<Mutex<View>>,
    groups: BTreeMap<String, Group>,
    member_of: HashMap<String, String>,
    previous: HashMap<String, String>,
    id_prefix: String,
    next_id: u64,
    next_generation: u64,
}

impl Actor {
    async fn run(mut self, mut ops: mpsc::UnboundedReceiver<Op>) {
        while let Some(op) = ops.recv().await {
            self.handle(op).await;
            self.publish();
        }
        for (_, group) in std::mem::take(&mut self.groups) {
            if let Some(playing) = group.stream {
                playing.stream.stop().await;
            }
        }
    }

    async fn handle(&mut self, op: Op) {
        match op {
            Op::Connect(conn) => self.connect(conn),
            Op::Disconnect(conn) => {
                if self.current_group(&conn).is_some() {
                    self.detach(conn.client_id()).await;
                    self.previous.remove(conn.client_id());
                }
            }
            Op::Unavailable(conn) => self.unavailable(conn).await,
            Op::Join {
                client_id,
                group_id,
                reply,
            } => {
                let _ = reply.send(self.join(&client_id, &group_id).await);
            }
            Op::Leave { client_id, reply } => {
                let _ = reply.send(self.leave(&client_id).await);
            }
            Op::Rename {
                group_id,
                name,
                reply,
            } => {
                let r = self.group_mut(&group_id).map(|g| {
                    g.name = name;
                    g.update_all();
                });
                let _ = reply.send(r);
            }
            Op::Play {
                group_id,
                source,
                reply,
            } => {
                let _ = reply.send(self.play(&group_id, source));
            }
            Op::QueueNext {
                group_id,
                source,
                reply,
            } => {
                let r = self.group_mut(&group_id).and_then(|g| match &g.stream {
                    Some(playing) => Ok(playing.stream.queue_next(source)?),
                    None => Err(GroupError::NotPlaying(group_id.clone())),
                });
                let _ = reply.send(r);
            }
            Op::Stop { group_id, reply } => {
                let r = match self.group_mut(&group_id) {
                    Ok(_) => {
                        self.stop(&group_id).await;
                        Ok(())
                    }
                    Err(e) => Err(e),
                };
                let _ = reply.send(r);
            }
            Op::Volume {
                group_id,
                volume,
                reply,
            } => {
                let r = self
                    .group_mut(&group_id)
                    .map(|g| g.command(PlayerCommand::volume(volume)));
                let _ = reply.send(r);
            }
            Op::Mute {
                group_id,
                mute,
                reply,
            } => {
                let r = self
                    .group_mut(&group_id)
                    .map(|g| g.command(PlayerCommand::mute(mute)));
                let _ = reply.send(r);
            }
            Op::Ended {
                group_id,
                generation,
            } => {
                let Some(group) = self.groups.get_mut(&group_id) else {
                    return;
                };
                if group
                    .stream
                    .as_ref()
                    .is_some_and(|p| p.generation == generation)
                {
                    tracing::info!(group_id, "group stream played out");
                    group.stream = None;
                    group.update_all();
                }
            }
        }
    }

    fn connect(&mut self, conn: Arc<Connection>) {
        let client_id = conn.client_id();
        let Some(group) = self
            .member_of
            .get(client_id)
            .and_then(|id| self.groups.get_mut(id))
        else {
            self.solo(conn);
            return;
        };
        // A reconnect: the new connection takes the old one's place.
        for m in &mut group.members {
            if m.client_id() == client_id {
                *m = conn.clone();
            }
        }
        group.update(&conn);
        if let Some(playing) = &group.stream
            && conn.has_role("player")
        {
            playing.stream.add_player(conn);
        }
    }

    async fn unavailable(&mut self, conn: Arc<Connection>) {
        let Some(group) = self.current_group(&conn) else {
            return;
        };
        let client_id = conn.client_id();
        let group_id = group.id.clone();
        if group.members.len() > 1 {
            tracing::info!(
                client_id,
                group_id,
                "unavailable, moving to a group of its own"
            );
            self.detach(client_id).await;
            self.previous.insert(client_id.to_owned(), group_id);
            self.solo(conn);
        } else if group.stream.is_some() {
            tracing::info!(client_id, group_id, "unavailable, stopping its group");
            self.stop(&group_id).await;
        }
    }

    async fn join(&mut self, client_id: &str, group_id: &str) -> Result<(), GroupError> {
        if !self.groups.contains_key(group_id) {
            return Err(GroupError::NoGroup(group_id.into()));
        }
        let Some(current) = self.member_of.get(client_id) else {
            return Err(GroupError::NoClient(client_id.into()));
        };
        if current == group_id {
            return Ok(());
        }
        let conn = self.detach(client_id).await.expect("a member");
        self.previous.remove(client_id);
        tracing::info!(client_id, group_id, "joining group");
        let group = self.groups.get_mut(group_id).expect("checked above");
        group.members.push(conn.clone());
        self.member_of.insert(client_id.into(), group_id.into());
        group.update(&conn);
        if let Some(playing) = &group.stream
            && conn.has_role("player")
        {
            playing.stream.add_player(conn);
        }
        Ok(())
    }

    async fn leave(&mut self, client_id: &str) -> Result<String, GroupError> {
        let Some(current) = self.member_of.get(client_id) else {
            return Err(GroupError::NoClient(client_id.into()));
        };
        if self.groups[current].members.len() == 1 {
            return Ok(current.clone());
        }
        let conn = self.detach(client_id).await.expect("a member");
        self.previous.remove(client_id);
        Ok(self.solo(conn))
    }

    fn play(&mut self, group_id: &str, source: Box<dyn LiveSource>) -> Result<(), GroupError> {
        let clock = self.clock;
        let generation = self.next_generation;
        self.next_generation += 1;
        let ops = self.ops.clone();
        let group = self.group_mut(group_id)?;
        if let Some(playing) = &group.stream {
            playing.stream.seek(source)?;
            return Ok(());
        }
        let stream = AudioStream::start(clock, source)?;
        let ended = stream.ended();
        let id = group_id.to_owned();
        tokio::spawn(async move {
            ended.await;
            if let Some(ops) = ops.upgrade() {
                let _ = ops.send(Op::Ended {
                    group_id: id,
                    generation,
                });
            }
        });
        group.stream = Some(Playing { stream, generation });
        group.update_all();
        let playing = group.stream.as_ref().expect("set above");
        for conn in group.members.iter().filter(|c| c.has_role("player")) {
            playing.stream.add_player(conn.clone());
        }
        tracing::info!(group_id, "group playing");
        Ok(())
    }

    /// Stops the group's stream, if it has one, and tells its members.
    async fn stop(&mut self, group_id: &str) {
        let Some(group) = self.groups.get_mut(group_id) else {
            return;
        };
        let Some(playing) = group.stream.take() else {
            return;
        };
        playing.stream.stop().await;
        group.update_all();
        tracing::info!(group_id, "group stopped");
    }

    /// Takes `client_id` out of its group, waiting until the group's
    /// stream has queued its last message for it. An emptied group is
    /// dropped.
    async fn detach(&mut self, client_id: &str) -> Option<Arc<Connection>> {
        let group_id = self.member_of.remove(client_id)?;
        let group = self.groups.get_mut(&group_id)?;
        let i = group
            .members
            .iter()
            .position(|m| m.client_id() == client_id)?;
        let conn = group.members.remove(i);
        if group.members.is_empty() {
            let group = self.groups.remove(&group_id).expect("found above");
            if let Some(playing) = group.stream {
                playing.stream.stop().await;
            }
        } else if let Some(playing) = &group.stream {
            let removed = playing.stream.remove_player(client_id);
            removed.await;
        }
        Some(conn)
    }

    /// Puts `conn` in a new, stopped group of its own, named after it.
    fn solo(&mut self, conn: Arc<Connection>) -> String {
        let id = format!("{}-{}", self.id_prefix, self.next_id);
        self.next_id += 1;
        let group = Group {
            id: id.clone(),
            name: conn.name().to_owned(),
            members: vec![conn.clone()],
            stream: None,
        };
        group.update(&conn);
        self.member_of
            .insert(conn.client_id().to_owned(), id.clone());
        self.groups.insert(id.clone(), group);
        id
    }

    /// The group `conn` is in, if it is still its client's connection.
    fn current_group(&self, conn: &Arc<Connection>) -> Option<&Group> {
        let group = self.groups.get(self.member_of.get(conn.client_id())?)?;
        group.is_member(conn).then_some(group)
    }

    fn group_mut(&mut self, group_id: &str) -> Result<&mut Group, GroupError> {
        self.groups
            .get_mut(group_id)
            .ok_or_else(|| GroupError::NoGroup(group_id.into()))
    }

    fn publish(&self) {
        let mut view = self.view.lock().unwrap_or_else(PoisonError::into_inner);
        view.groups = self
            .groups
            .iter()
            .map(|(id, g)| (id.clone(), g.info()))
            .collect();
        view.member_of.clone_from(&self.member_of);
        view.previous.clone_from(&self.previous);
    }
}

#[cfg(test)]
mod tests;
