use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::Arc,
};

use thiserror::Error;
use tokio_etcd_grpc_client::{self as pb};

use super::{Key, WatchConfig};
use crate::{
    ids::{IdFastHasherBuilder, Revision, Version},
    kv::{KeyValue, ResponseHeader},
    LeaseId, WatchId,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WatcherSyncState {
    /// The watch request for this watcher has yet to be sent to the server.
    Unsynced,
    /// The watch request has been sent to the server, but we have yet to receive an acknowledgement from the server
    /// that the watch was created.
    Syncing,
    /// The watch request has been acknowledged by the server.
    Synced,
}

struct WatcherState {
    id: WatchId,
    config: WatchConfig,
    sync_state: WatcherSyncState,
}

// A subset of the WatcherState, which contains mutable references to fields that are safe to update.
struct UpdatableWatcherState<'a> {
    sync_state: &'a mut WatcherSyncState,
    start_revision: &'a mut Option<Revision>,
}

#[derive(Debug)]
pub struct WatcherEvent {
    /// The key (in bytes) that has been updated.
    pub key: Key,
    /// The value that the key has been updated to.
    pub value: WatcherValue,
    /// The previous value. Only set if the watcher was configured to receive previous values.
    pub prev_value: WatcherValue,
}

#[derive(Clone, Debug)]
pub enum WatcherValue {
    Set {
        /// The value that the key has been set to.
        value: Arc<[u8]>,
        /// The revision that the key was modified.
        mod_revision: Revision,
        /// The revision that the key was created.
        create_revision: Revision,
        /// Version is the version of the key. A deletion resets the version to zero and any modification of the key
        /// increases its version.
        version: Version,
        /// lease is the ID of the lease that attached to key. When the attached lease expires, the key will be deleted.
        /// If lease is None, then no lease is attached to the key.
        lease_id: Option<LeaseId>,
    },
    Unset {
        /// The revision of the key that was deleted, if it is known.
        ///
        /// If the key was deleted before we started watching, we won't have the revision of the key that was deleted.
        mod_revision: Option<Revision>,
    },
}

impl WatcherValue {
    pub(crate) fn from_kv(kv: pb::KeyValue) -> Self {
        Self::Set {
            value: kv.value.into(),
            mod_revision: Revision(kv.mod_revision),
            create_revision: Revision(kv.create_revision),
            version: Version(kv.version),
            lease_id: LeaseId::new(kv.lease),
        }
    }
}

impl UpdatableWatcherState<'_> {
    fn handle_event(&mut self, event: pb::Event) -> WatcherEvent {
        let event_type = event.r#type();
        let kv = KeyValue::from(event.kv.expect("invariant: kv is always present"));
        let prev_kv = event.prev_kv.map(KeyValue::from);

        *self.start_revision = Some(
            kv.mod_revision
                .max(self.start_revision.unwrap_or_default())
                .next(),
        );

        WatcherEvent {
            key: kv.key,
            value: match event_type {
                pb::EventType::Put => WatcherValue::Set {
                    value: kv.value.into(),
                    mod_revision: kv.mod_revision,
                    create_revision: kv.create_revision,
                    version: kv.version,
                    lease_id: kv.lease,
                },
                pb::EventType::Delete => WatcherValue::Unset {
                    mod_revision: Some(kv.mod_revision),
                },
            },
            prev_value: match prev_kv {
                Some(kv) => WatcherValue::Set {
                    value: kv.value.into(),
                    mod_revision: kv.mod_revision,
                    create_revision: kv.create_revision,
                    version: kv.version as _,
                    lease_id: kv.lease,
                },
                None => WatcherValue::Unset { mod_revision: None },
            },
        }
    }
}

impl WatcherState {
    fn initial_watch_request(&self) -> pb::WatchRequest {
        let WatchConfig {
            events,
            key,
            prev_kv,
            range_end,
            start_revision,
        } = &self.config;

        pb::WatchRequest {
            request_union: Some(pb::watch_request::RequestUnion::CreateRequest(
                tokio_etcd_grpc_client::WatchCreateRequest {
                    watch_id: self.id.get() as _,
                    fragment: true,
                    progress_notify: true,
                    // configured values:
                    key: key.as_vec(),
                    range_end: range_end.as_ref().map(|x| x.as_vec()).unwrap_or_default(),
                    start_revision: start_revision.unwrap_or_default().0,
                    filters: events.for_filters_proto(),
                    prev_kv: *prev_kv,
                },
            )),
        }
    }
}

pub(crate) struct WatcherFsm {
    next_id: WatchId,
    states: HashMap<WatchId, WatcherState, IdFastHasherBuilder>,
    unsynced_watchers: VecDeque<WatchId>,
    syncing_watchers: VecDeque<WatchId>,
    pending_cancels: VecDeque<WatchId>,
    fragmented_responses: HashMap<WatchId, pb::WatchResponse, IdFastHasherBuilder>,
    concurrent_sync_limit: usize,
}

impl WatcherFsm {
    pub(crate) fn new(concurrent_sync_limit: usize) -> Self {
        WatcherFsm {
            next_id: WatchId(1),
            states: Default::default(),
            unsynced_watchers: Default::default(),
            syncing_watchers: Default::default(),
            fragmented_responses: Default::default(),
            pending_cancels: Default::default(),
            concurrent_sync_limit,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.states.len()
    }

    /// Begins watching the given key after the specified revision. Returns the `WatchId` that was created to watch
    /// the key.
    ///
    /// The watcher starts out as unsynced, and will progress towards sync as the connecion managing this
    /// fsm progresses.
    pub(crate) fn add_watcher(&mut self, config: WatchConfig) -> WatchId {
        let id = self.next_id;
        self.next_id = id.next();
        self.states.insert(
            id,
            WatcherState {
                id,
                config,
                sync_state: WatcherSyncState::Unsynced,
            },
        );
        self.unsynced_watchers.push_back(id);
        id
    }

    /// Returns the next watch request to send to the etcd server in order to sync the connection.
    ///
    /// This function will try to send unsynced watchers first, limiting the number of in-flight requests to sync
    /// watchers depending on config.
    ///
    /// Otherwise, it will send cancel requests for watchers that have been cancelled.
    pub(crate) fn next_watch_request_to_send(&mut self) -> Option<pb::WatchRequest> {
        // If we have watchers that are unsynced, we'll try to sync them first.
        if self.syncing_watchers.len() < self.concurrent_sync_limit {
            if let Some(&unsynced_id) = self.unsynced_watchers.front() {
                self.update_watcher(unsynced_id, |state| {
                    assert!(matches!(state.sync_state, WatcherSyncState::Unsynced));
                    *state.sync_state = WatcherSyncState::Syncing;
                })
                .expect("invariant: found unsynced watcher, but watcher not found");

                return Some(self.states[&unsynced_id].initial_watch_request());
            }
        }

        // Otherwise, let's send any pending cancel requests.
        self.pending_cancels.pop_front().map(|w| w.cancel_request())
    }

    pub(crate) fn cancel_watcher(&mut self, id: WatchId) -> Option<WatchConfig> {
        self.cancel_watcher_with_source(id, CancelSource::Client)
    }

    /// Cancels a watcher by its watch id.
    fn cancel_watcher_with_source(
        &mut self,
        id: WatchId,
        cancel_source: CancelSource,
    ) -> Option<WatchConfig> {
        let WatcherState {
            config, sync_state, ..
        } = self.states.remove(&id)?;

        self.fragmented_responses.remove(&id);
        self.apply_to_state_container(sync_state, |c| c.retain(|item| *item != id));

        match cancel_source {
            CancelSource::Client => {
                // If we're not in the unsynced state, it means that we've already sent a watch request
                // to the etcd server, and we should send a cancel request. Otherwise, we can just remove
                // the watcher from the set, as the server never knew about it.
                if sync_state != WatcherSyncState::Unsynced {
                    self.pending_cancels.push_back(id);
                }
            }
            CancelSource::Server => {}
        }

        Some(config)
    }

    fn apply_to_state_container(
        &mut self,
        state: WatcherSyncState,
        func: impl FnOnce(&mut VecDeque<WatchId>),
    ) {
        match state {
            WatcherSyncState::Unsynced => func(&mut self.unsynced_watchers),
            WatcherSyncState::Syncing => func(&mut self.syncing_watchers),
            WatcherSyncState::Synced => {}
        }
    }

    /// Updates the state of a watcher, and returns a reference to the updated state.
    ///
    /// If the watcher is not found, this function will return None.
    ///
    /// This function should be the only way to update the state of a watcher, as it ensures that the watcher
    /// is moved to the correct container depending on its sync state.
    fn update_watcher<T>(
        &mut self,
        id: WatchId,
        f: impl FnOnce(UpdatableWatcherState) -> T,
    ) -> Option<T> {
        let state = self.states.get_mut(&id)?;
        let prev_sync_state = state.sync_state;
        let updatable_state = UpdatableWatcherState {
            sync_state: &mut state.sync_state,
            start_revision: &mut state.config.start_revision,
        };
        let result = f(updatable_state);
        let next_sync_state = state.sync_state;

        if prev_sync_state != next_sync_state {
            self.apply_to_state_container(prev_sync_state, |target| {
                // Check if the watcher is at the front of the vecdeq, and if so, we can just pop it.
                // Generally, this should be the case, as we'll be processing watchers in order.
                if target.front() == Some(&id) {
                    target.pop_front();
                    return;
                }

                // Otherwise, we'll need to find the item in the vecdeq, and remove it.
                target.retain(|i| i != &id);
            });
            self.apply_to_state_container(next_sync_state, |target| target.push_back(id));
        }

        Some(result)
    }

    /// Resets the set for a new connection, which ultimately means:
    ///
    /// - Resetting all the sync states to unsynced.
    /// - Clearing all fragmented responses.
    /// - Clearing all pending cancels, since we won't re-start the watchers that were cancelled on the new connection.
    pub(crate) fn reset_for_new_connection(&mut self) {
        self.syncing_watchers.clear();
        self.unsynced_watchers.clear();
        self.fragmented_responses.clear();
        self.pending_cancels.clear();

        for state in self.states.values_mut() {
            // This is safe to mutate outside of update_watcher, as we've reset the containers above.
            state.sync_state = WatcherSyncState::Unsynced;
        }
        self.unsynced_watchers.extend(self.states.keys());
    }

    /// Watch responses can be fragmented, so we'll need to merge them together before we can process them.
    ///
    /// Returns:
    ///  - Some(WatchResponse): if the response was a complete response, and we've merged all the fragments.
    ///  - None: if the response was a fragment, and we're waiting for more fragments.
    fn try_merge_fragmented_response(
        &mut self,
        response: pb::WatchResponse,
    ) -> Option<pb::WatchResponse> {
        let id = WatchId(response.watch_id as _);

        match (self.fragmented_responses.entry(id), response.fragment) {
            // We have an existing fragment, and this is a fragment, so we'll merge them.
            (Entry::Occupied(mut ent), true) => {
                ent.get_mut().events.extend(response.events);
                None
            }
            // We have a fragment, but this is a complete response, so we'll remove the fragment and return the
            // complete response.
            (Entry::Occupied(ent), false) => {
                let mut existing = ent.remove();
                existing.events.extend(response.events);
                Some(existing)
            }
            // We don't have a fragment, and this is a fragment, so we'll just insert it.
            (Entry::Vacant(ent), true) => {
                ent.insert(response);
                None
            }
            // We don't have a fragment, and this is a complete response, so we'll just return it.
            (Entry::Vacant(_), false) => Some(response),
        }
    }

    /// Processes a watch response from the etcd server.
    ///
    /// Progresses the finite state machine forward.
    pub(crate) fn process_watch_response(
        &mut self,
        response: pb::WatchResponse,
    ) -> Option<(WatchId, WatchResponse)> {
        let id = WatchId(response.watch_id as _);

        // There is a chance that we may receive a watch response for a watcher we already don't care about.
        // In this case, we'll just ignore the response, since we've already cancelled the watcher.
        if !self.states.contains_key(&id) {
            tracing::info!("received response for unknown watcher: {:?} - ignoring", id);
            return None;
        }

        // Handle fragmented responses by merging them together if necessary.
        let Some(response) = self.try_merge_fragmented_response(response) else {
            return None;
        };

        if response.canceled {
            self.cancel_watcher_with_source(id, CancelSource::Server);

            Some((
                id,
                WatchResponse::CancelledByServer(CancelledByServer {
                    reason: response.cancel_reason.into(),
                }),
            ))
        } else {
            let response = self.update_watcher(id, |mut s| {
                if response.compact_revision != 0 {
                    tracing::warn!(
                        "when trying to sync watcher {:?} at revision {:?}, etcd server returned a compact revision of
                        {}, restarting watcher at compact revision.",
                        id, s.start_revision, response.compact_revision
                    );
                    *s.sync_state = WatcherSyncState::Unsynced;
                    // fixme: log compact revision properly, we need to re-fetch the entire key potentially?
                    *s.start_revision = Some(Revision(response.compact_revision));
                    return WatchResponse::CompactRevision { revision: Revision(response.compact_revision) };
                }

                let header = ResponseHeader::from(response
                        .header
                        .expect("invariant: header is always present"));

                // the server has acknowledged the watcher, so we can mark it as synced.
                if response.created {
                    tracing::info!("watcher {:?} synced", id);
                    *s.sync_state = WatcherSyncState::Synced;
                } else {
                    *s.start_revision = Some(header.revision.next());
                }

                if response.is_progress_notify() {
                    return WatchResponse::Progress { revision: header.revision };
                }

                let mut events = Vec::with_capacity(response.events.len());
                for event in response.events {
                    events.push(s.handle_event(event));
                }

                WatchResponse::Events { events, revision: header.revision }
            })?;

            Some((id, response))
        }
    }
}

enum CancelSource {
    /// The client requested the watcher to be cancelled.
    Client,
    /// The server cancelled the watcher.
    Server,
}

#[derive(Debug)]
pub enum WatchResponse {
    /// The watcher was cancelled by the server.
    CancelledByServer(CancelledByServer),
    /// The watcher emitted the following events.
    Events {
        events: Vec<WatcherEvent>,
        revision: Revision,
    },
    /// The watcher notified progress which updated the revision.
    Progress { revision: Revision },
    /// The watcher revision was compacted.
    CompactRevision { revision: Revision },
}

impl WatchResponse {
    /// Returns `true` if the processed watch response is [`Cancelled`].
    ///
    /// [`Cancelled`]: ProcessedWatchResponse::Cancelled
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::CancelledByServer(..))
    }
}

#[derive(Debug, Error, Clone)]
#[error("watch cancelled by server: {reason}")]
pub struct CancelledByServer {
    pub reason: Arc<str>,
}

impl WatchId {
    fn next(self) -> Self {
        WatchId(self.0 + 1)
    }

    fn cancel_request(&self) -> pb::WatchRequest {
        pb::WatchRequest {
            request_union: Some(pb::watch_request::RequestUnion::CancelRequest(
                tokio_etcd_grpc_client::WatchCancelRequest {
                    watch_id: self.get() as _,
                },
            )),
        }
    }
}
