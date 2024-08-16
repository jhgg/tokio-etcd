use tokio_etcd_grpc_client::{self as pb};
use tonic::Status;

use crate::{lease::LeaseHandle, watcher::Key};

pub struct KVClient {
    client: pb::KvClient<pb::AuthedChannel>,
}

impl KVClient {
    pub(crate) fn new(client: pb::KvClient<pb::AuthedChannel>) -> Self {
        Self { client }
    }

    pub fn put(&self, key: impl Into<Key>, value: impl Into<Vec<u8>>) -> PutRequest {
        PutRequest::new(self.client.clone(), key.into(), value.into())
    }
}

mod ts {
    pub struct Unset;
    pub struct True;
    pub struct Set<T>(pub(crate) T);
}

pub struct PutRequest<TPrev = ts::Unset, TLease = ts::Unset> {
    client: pb::KvClient<pb::AuthedChannel>,
    key: Key,
    value: Vec<u8>,
    lease: TLease,
    prev: TPrev,
}

impl PutRequest<ts::Unset, ts::Unset> {
    fn new(client: pb::KvClient<pb::AuthedChannel>, key: Key, value: Vec<u8>) -> Self {
        Self {
            client,
            key,
            value,
            lease: ts::Unset,
            prev: ts::Unset,
        }
    }
}

impl<TLease> PutRequest<ts::Unset, TLease> {
    pub fn with_prev_kv(self) -> PutRequest<ts::True, TLease> {
        PutRequest {
            client: self.client,
            key: self.key,
            value: self.value,
            lease: self.lease,
            prev: ts::True,
        }
    }

    pub async fn execute(self) -> Result<(), Status> {
        todo!()
    }
}

impl<TLease> PutRequest<ts::True, TLease> {
    // fixme: Ok should be something more.
    pub async fn execute(self) -> Result<Vec<u8>, Status> {
        todo!()
    }
}

impl<TPrev> PutRequest<TPrev, ts::Unset> {
    // xx: should we support a raw LeaseId too?
    pub fn with_lease(self, lease: &LeaseHandle) -> PutRequest<TPrev, ts::Set<LeaseHandle>> {
        PutRequest {
            client: self.client,
            key: self.key,
            value: self.value,
            lease: ts::Set(lease.clone()),
            prev: self.prev,
        }
    }
}
