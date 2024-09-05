use std::time::Duration;

use crate::{
    ids::{ClusterId, LeaseId, MemberId, RaftTerm, Revision, Version},
    utils::backoff::ExponentialBackoff,
    watcher::Key,
};

use tokio::time::timeout;
use tokio_etcd_grpc_client::{self as pb};
use tonic::Status;
use util::is_retryable_error_code;

mod put;
mod ts;
mod util;

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

    /// Convenience method to get a single key. For more advanced options, use [`KVClient::range_request`].
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<KeyValue>, Status> {
        let request = pb::RangeRequest {
            key: key.into().into_vec(),
            ..Default::default()
        };

        let mut client = self.client.clone();
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(50), Duration::from_secs(2));

        match timeout(Duration::from_secs(10), async move {
            loop {
                match client.range(request.clone()).await {
                    Ok(response) => {
                        return Ok(response
                            .into_inner()
                            .kvs
                            .into_iter()
                            .next()
                            .map(KeyValue::from));
                    }
                    Err(status) => {
                        if is_retryable_error_code(status.code()) {
                            backoff.delay().await;
                        } else {
                            return Err(status);
                        }
                    }
                }
            }
        })
        .await
        {
            Ok(res) => res,
            Err(_) => Err(Status::deadline_exceeded("request timed out")),
        }
    }

    pub fn range_request(&self) {
        todo!()
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
