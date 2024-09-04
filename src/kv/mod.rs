use crate::{
    ids::{ClusterId, LeaseId, MemberId, RaftTerm, Revision, Version},
    watcher::Key,
};

use tokio_etcd_grpc_client::{self as pb};

mod put;
mod ts;

pub struct KVClient {
    client: pb::KvClient<pb::AuthedChannel>,
}

impl KVClient {
    pub(crate) fn new(client: pb::KvClient<pb::AuthedChannel>) -> Self {
        Self { client }
    }

    pub fn put(&self, key: impl Into<Key>) -> put::PutRequest {
        put::PutRequest::new(self.client.clone(), key.into())
    }
}

pub struct KeyValue {
    pub key: Key,
    pub create_revision: Revision,
    pub mod_revision: Revision,
    pub version: Version,
    pub value: Vec<u8>,
    pub lease: Option<LeaseId>,
}

impl From<pb::KeyValue> for KeyValue {
    fn from(value: pb::KeyValue) -> Self {
        Self {
            key: value.key.into(),
            value: value.value,
            create_revision: Revision(value.create_revision),
            mod_revision: Revision(value.mod_revision),
            lease: LeaseId::new(value.lease),
            version: Version(value.version),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResponseHeader {
    pub cluster_id: ClusterId,
    /// member_id is the ID of the member which sent the response.
    pub member_id: MemberId,
    /// revision is the key-value store revision when the request was applied, and it's
    /// unset (so 0) in case of calls not interacting with key-value store.
    /// For watch progress responses, the header.revision indicates progress. All future events
    /// received in this stream are guaranteed to have a higher revision number than the
    /// header.revision number.
    pub revision: Revision,
    /// raft_term is the raft term when the request was applied.
    pub raft_term: RaftTerm,
}

impl From<pb::ResponseHeader> for ResponseHeader {
    fn from(value: pb::ResponseHeader) -> Self {
        Self {
            revision: Revision(value.revision),
            cluster_id: ClusterId(value.cluster_id),
            member_id: MemberId(value.member_id),
            raft_term: RaftTerm(value.raft_term),
        }
    }
}
