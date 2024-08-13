use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::Arc,
};

use thiserror::Error;
use tokio_etcd_grpc_client::{
    watch_request, Event, EventType, KeyValue, WatchRequest, WatchResponse, PROGRESS_WATCH_ID,
};

use super::Key;
use crate::{ids::IdFastHasherBuilder, LeaseId, WatchId};

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
    key: Key,
    sync_state: WatcherSyncState,
    /// The revision which we last received from the etcd server either by virtue of getting a watch response,
    /// or by a progress notification.
    ///
    /// This value is distinct from the WatcherValue's revision, as it represents the the latest revision from etcd
    /// that we know about, and not the revision of the value itself, which may be older, and unable to be watched
    /// (as it may have been compacted).
    revision: i64,
}

// A subset of the WatcherState, which contains mutable references to fields that are safe to update.
struct UpdatableWatcherState<'a> {
    sync_state: &'a mut WatcherSyncState,
    revision: &'a mut i64,
}

pub struct WatcherEvent {
    /// The key (in bytes) that has been updated.
    pub key: Arc<[u8]>,
    /// The value that the key has been updated to.
    pub value: WatcherValue,
}

#[derive(Clone, Debug)]
pub enum WatcherValue {
    Set {
        /// The value that the key has been set to.
        value: Arc<[u8]>,
        /// The revision that the key was modified.
        mod_revision: u64,
        /// The revision that the key was created.
        create_revision: u64,
        /// Version is the version of the key. A deletion resets the version to zero and any modification of the key
        /// increases its version.
        version: u64,
        /// lease is the ID of the lease that attached to key. When the attached lease expires, the key will be deleted.
        /// If lease is None, then no lease is attached to the key.
        lease_id: Option<LeaseId>,
    },
    Unset {
        /// The revision of the key that was deleted, if it is known.
        ///
        /// If the key was deleted before we started watching, we won't have the revision of the key that was deleted.
        mod_revision: Option<i64>,
    },
}

impl WatcherValue {
    pub(crate) fn from_kv(kv: KeyValue) -> Self {
        Self::Set {
            value: kv.value.into(),
            mod_revision: kv.mod_revision as _,
            create_revision: kv.create_revision as _,
            version: kv.version as _,
            lease_id: LeaseId::new(kv.lease),
        }
    }
}

impl UpdatableWatcherState<'_> {
    fn handle_event(&mut self, event: Event) -> WatcherEvent {
        let event_type = event.r#type();
        let kv = event.kv.expect("invariant: kv is always present");
        *self.revision = kv.mod_revision.max(*self.revision);

        WatcherEvent {
            key: kv.key.into(),
            value: match event_type {
                EventType::Put => WatcherValue::Set {
                    value: kv.value.into(),
                    mod_revision: kv.mod_revision as _,
                    create_revision: kv.create_revision as _,
                    version: kv.version as _,
                    lease_id: LeaseId::new(kv.lease),
                },
                EventType::Delete => WatcherValue::Unset {
                    mod_revision: Some(kv.mod_revision),
                },
            },
        }
    }
}

impl WatcherState {
    fn initial_watch_request(&self) -> WatchRequest {
        WatchRequest {
            request_union: Some(watch_request::RequestUnion::CreateRequest(
                tokio_etcd_grpc_client::WatchCreateRequest {
                    key: self.key.0.clone().into_vec(),
                    range_end: vec![],
                    start_revision: self.revision + 1,
                    progress_notify: true,
                    filters: vec![],
                    prev_kv: false,
                    watch_id: self.id.get() as _,
                    fragment: true,
                },
            )),
        }
    }
}

pub(crate) struct WatcherFsm {
    next_id: WatchId,
    // fixme: move this out of watcher map and into the worker.
    states: HashMap<WatchId, WatcherState, IdFastHasherBuilder>,
    unsynced_watchers: VecDeque<WatchId>,
    syncing_watchers: VecDeque<WatchId>,
    pending_cancels: VecDeque<WatchId>,
    fragmented_responses: HashMap<WatchId, WatchResponse, IdFastHasherBuilder>,
    concurrent_sync_limit: usize,
}

impl WatcherFsm {
    pub(crate) fn new(concurrent_sync_limit: usize) -> Self {
        WatcherFsm {
            next_id: WatchId(0),
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
    pub(crate) fn add_watcher(&mut self, key: Key, revision: i64) -> WatchId {
        let id = self.next_id;
        self.next_id = id.next();
        self.states.insert(
            id,
            WatcherState {
                key,
                sync_state: WatcherSyncState::Unsynced,
                revision,
                id,
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
    pub(crate) fn next_watch_request_to_send(&mut self) -> Option<WatchRequest> {
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

    pub(crate) fn cancel_watcher(&mut self, watch_id: WatchId) -> Option<Key> {
        self.cancel_watcher_with_source(watch_id, CancelSource::Client)
    }

    /// Cancels a watcher by its watch id.
    fn cancel_watcher_with_source(
        &mut self,
        id: WatchId,
        cancel_source: CancelSource,
    ) -> Option<Key> {
        let WatcherState {
            key, sync_state, ..
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

        Some(key)
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
        watch_id: WatchId,
        f: impl FnOnce(UpdatableWatcherState) -> T,
    ) -> Option<T> {
        let state = self.states.get_mut(&watch_id)?;
        let prev_sync_state = state.sync_state;
        let updatable_state = UpdatableWatcherState {
            sync_state: &mut state.sync_state,
            revision: &mut state.revision,
        };
        let result = f(updatable_state);
        let next_sync_state = state.sync_state;

        if prev_sync_state != next_sync_state {
            self.apply_to_state_container(prev_sync_state, |target| {
                // Check if the watcher is at the front of the vecdeq, and if so, we can just pop it.
                // Generally, this should be the case, as we'll be processing watchers in order.
                if target.front() == Some(&watch_id) {
                    target.pop_front();
                    return;
                }

                // Otherwise, we'll need to find the item in the vecdeq, and remove it.
                target.retain(|i| i != &watch_id);
            });
            self.apply_to_state_container(next_sync_state, |target| target.push_back(watch_id));
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
    fn try_merge_fragmented_response(&mut self, response: WatchResponse) -> Option<WatchResponse> {
        let watch_id = WatchId(response.watch_id as _);

        match (self.fragmented_responses.entry(watch_id), response.fragment) {
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
        response: WatchResponse,
    ) -> Option<(WatchId, ProcessedWatchResponse)> {
        let watch_id = WatchId(response.watch_id as _);

        // There is a chance that we may receive a watch response for a watcher we already don't care about.
        // In this case, we'll just ignore the response, since we've already cancelled the watcher.
        if !self.states.contains_key(&watch_id) {
            tracing::info!(
                "received response for unknown watcher: {:?} - ignoring",
                response.watch_id
            );
            return None;
        }

        // Handle fragmented responses by merging them together if necessary.
        let Some(response) = self.try_merge_fragmented_response(response) else {
            return None;
        };

        if response.canceled {
            self.cancel_watcher_with_source(watch_id, CancelSource::Server);

            Some((
                watch_id,
                ProcessedWatchResponse::Cancelled(WatchCancelledByServer {
                    reason: response.cancel_reason.into(),
                }),
            ))
        } else {
            let response = self.update_watcher(watch_id, |mut s| {
                if response.compact_revision != 0 {
                    tracing::warn!(
                        "when trying to sync watcher {:?} at revision {}, etcd server returned a compact revision of
                        {}, restarting watcher at compact revision.",
                        watch_id, s.revision, response.compact_revision
                    );
                    *s.sync_state = WatcherSyncState::Unsynced;
                    // fixme: log compact revision properly, we need to re-fetch the entire key potentially?
                    *s.revision = response.compact_revision;
                    return ProcessedWatchResponse::CompactRevision { revision: response.compact_revision };
                }

                let revision = response
                        .header
                        .expect("invariant: header is always present")
                        .revision;

                // the server has acknowledged the watcher, so we can mark it as synced.
                if response.created {
                    tracing::info!("watcher {:?} synced", watch_id);
                    *s.sync_state = WatcherSyncState::Synced;
                } else {
                    *s.revision = revision;
                }

                if response.is_progress_notify() {
                    return ProcessedWatchResponse::Progress { revision };
                }

                let mut events = Vec::with_capacity(response.events.len());
                for event in response.events {
                    events.push(s.handle_event(event));
                }

                ProcessedWatchResponse::Events { events, revision }
            })?;

            Some((watch_id, response))
        }
    }
}

enum CancelSource {
    /// The client requested the watcher to be cancelled.
    Client,
    /// The server cancelled the watcher.
    Server,
}

pub enum ProcessedWatchResponse {
    /// The watcher was cancelled by the server.
    Cancelled(WatchCancelledByServer),
    /// The watcher emitted the following events.
    Events {
        events: Vec<WatcherEvent>,
        revision: i64,
    },
    /// The watcher notified progress which updated the revision.
    Progress { revision: i64 },
    /// The watcher revision was compacted.
    CompactRevision { revision: i64 },
}

#[derive(Debug, Error, Clone)]
#[error("watch cancelled by server: {reason}")]
pub struct WatchCancelledByServer {
    pub reason: Arc<str>,
}

impl WatchId {
    fn next(self) -> Self {
        WatchId(self.0 + 1)
    }

    fn cancel_request(&self) -> WatchRequest {
        WatchRequest {
            request_union: Some(watch_request::RequestUnion::CancelRequest(
                tokio_etcd_grpc_client::WatchCancelRequest {
                    watch_id: self.get() as _,
                },
            )),
        }
    }
}
