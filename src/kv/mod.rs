use std::fmt::Display;

use thiserror::Error;
use tokio_etcd_grpc_client::{self as pb, Member};
use tonic::Status;
use ts::OptionOrIgnored;

use crate::{lease::LeaseHandle, watcher::Key, LeaseId};

pub struct KVClient {
    client: pb::KvClient<pb::AuthedChannel>,
}

impl KVClient {
    pub(crate) fn new(client: pb::KvClient<pb::AuthedChannel>) -> Self {
        Self { client }
    }

    pub fn put(&self, key: impl Into<Key>) -> PutRequest {
        PutRequest::new(self.client.clone(), key.into())
    }
}

mod ts {
    use std::marker::PhantomData;

    pub struct Unset<T>(PhantomData<T>);

    impl<T> Unset<T> {
        pub(super) fn new() -> Self {
            Self(PhantomData)
        }
    }

    pub struct Ignored<T>(PhantomData<T>);

    impl<T> Ignored<T> {
        pub(super) fn new() -> Self {
            Self(PhantomData)
        }
    }
    pub struct Set<T>(pub(crate) T);

    impl From<Unset<bool>> for bool {
        fn from(_: Unset<bool>) -> Self {
            false
        }
    }

    impl From<Set<()>> for bool {
        fn from(_: Set<()>) -> Self {
            true
        }
    }

    impl<T> From<Set<T>> for Option<T> {
        fn from(value: Set<T>) -> Self {
            Some(value.0)
        }
    }

    impl<T> From<Unset<T>> for Option<T> {
        fn from(value: Unset<T>) -> Self {
            None
        }
    }

    impl<T> From<Ignored<T>> for Option<T> {
        fn from(value: Ignored<T>) -> Self {
            None
        }
    }

    pub(super) enum OptionOrIgnored<T> {
        Some(T),
        None,
        Ignored,
    }

    impl<T> From<Set<T>> for OptionOrIgnored<T> {
        fn from(value: Set<T>) -> Self {
            OptionOrIgnored::Some(value.0)
        }
    }

    impl<T> From<Unset<T>> for OptionOrIgnored<T> {
        fn from(value: Unset<T>) -> Self {
            OptionOrIgnored::None
        }
    }

    impl<T> From<Ignored<T>> for OptionOrIgnored<T> {
        fn from(value: Ignored<T>) -> Self {
            OptionOrIgnored::Ignored
        }
    }
}

pub struct PutRequest<
    TValue = ts::Unset<Vec<u8>>,
    TPrev = ts::Unset<bool>,
    TLease = ts::Unset<LeaseIdOrHandle>,
> {
    client: pb::KvClient<pb::AuthedChannel>,
    key: Key,
    value: TValue,
    lease: TLease,
    prev: TPrev,
}

impl PutRequest<ts::Unset<Vec<u8>>, ts::Unset<bool>, ts::Unset<LeaseIdOrHandle>> {
    fn new(client: pb::KvClient<pb::AuthedChannel>, key: Key) -> Self {
        Self {
            client,
            key,
            value: ts::Unset::new(),
            lease: ts::Unset::new(),
            prev: ts::Unset::new(),
        }
    }
}

impl<TPrev, TLease> PutRequest<ts::Unset<Vec<u8>>, TPrev, TLease> {
    /// Sets the key to the given value.
    pub fn with_value(
        self,
        value: impl Into<Vec<u8>>,
    ) -> PutRequest<ts::Set<Vec<u8>>, TPrev, TLease> {
        PutRequest {
            client: self.client,
            key: self.key,
            lease: self.lease,
            prev: self.prev,
            value: ts::Set(value.into()),
        }
    }
}

impl<TPrev> PutRequest<ts::Unset<Vec<u8>>, TPrev, ts::Unset<LeaseIdOrHandle>> {
    /// Updates the lease on the key, but retains the previous value.
    ///
    /// Will return an error upon execution if the key does not exist.
    ///
    /// You can either provide a raw [`LeaseId`] or a [`LeaseHandle`], however,
    /// if the handle refers to a lease which is revoked, an error will be returned upon
    /// execution.
    pub fn only_updating_lease(
        self,
        lease: impl Into<LeaseIdOrHandle>,
    ) -> PutRequest<ts::Ignored<Vec<u8>>, TPrev, ts::Set<LeaseIdOrHandle>> {
        PutRequest {
            client: self.client,
            key: self.key,
            lease: ts::Set(lease.into()),
            prev: self.prev,
            value: ts::Ignored::new(),
        }
    }
}

#[derive(Debug, Error)]
enum PutError {
    #[error("lease handle has been revoked.")]
    LeaseHandleRevoked,

    #[error("grpc error: {0:?}")]
    GrpcError(#[from] Status),
}

impl From<LeaseHandleRevoked> for PutError {
    fn from(_: LeaseHandleRevoked) -> Self {
        Self::LeaseHandleRevoked
    }
}

impl<TValue, TPrev, TLease> PutRequest<TValue, TPrev, TLease>
where
    TValue: Into<ts::OptionOrIgnored<Vec<u8>>>,
    TPrev: Into<bool>,
    TLease: Into<ts::OptionOrIgnored<LeaseIdOrHandle>>,
{
    async fn raw_execute(mut self) -> Result<pb::PutResponse, PutError> {
        let value: OptionOrIgnored<Vec<u8>> = self.value.into();
        let (value, ignore_value) = match value {
            OptionOrIgnored::Some(value) => (value, false),
            // fixme: we shouldn't need this case? :(
            OptionOrIgnored::None => unreachable!(),
            OptionOrIgnored::Ignored => (vec![], true),
        };
        let lease: OptionOrIgnored<LeaseIdOrHandle> = self.lease.into();
        let (lease, ignore_lease) = match lease {
            // fixme: this is wrong,
            OptionOrIgnored::Some(x) => (x.get_lease_id()?.get_i64(), false),
            OptionOrIgnored::None => (0i64, false),
            OptionOrIgnored::Ignored => (0i64, true),
        };

        let request = pb::PutRequest {
            key: self.key.into_vec(),
            value,
            ignore_value,
            lease,
            ignore_lease,
            prev_kv: self.prev.into(),
        };

        let response = self.client.put(request).await?;
        Ok(response.into_inner())
    }
}

impl<TValue, TLease> PutRequest<TValue, ts::Unset<bool>, TLease>
where
    TValue: Into<ts::OptionOrIgnored<Vec<u8>>>,
    TLease: Into<ts::OptionOrIgnored<LeaseIdOrHandle>>,
{
    /// Return the previous [`KeyValue`] upon execution before changing it.
    pub fn return_prev_kv(self) -> PutRequest<TValue, ts::Set<()>, TLease> {
        PutRequest {
            client: self.client,
            key: self.key,
            value: self.value,
            lease: self.lease,
            prev: ts::Set(()),
        }
    }

    pub async fn execute(self) -> Result<PutResponse<()>, PutError> {
        let response = self.raw_execute().await?;

        Ok(PutResponse {
            header: response
                .header
                .expect("invariant: header must always be present")
                .into(),
            prev: (),
        })
    }
}

impl<TValue, TLease> PutRequest<TValue, ts::Set<()>, TLease>
where
    TValue: Into<ts::OptionOrIgnored<Vec<u8>>>,
    TLease: Into<ts::OptionOrIgnored<LeaseIdOrHandle>>,
{
    // fixme: Ok should be something more.
    pub async fn execute(self) -> Result<PutResponse<Option<KeyValue>>, PutError> {
        let response = self.raw_execute().await?;

        Ok(PutResponse {
            header: response
                .header
                .expect("invariant: header must always be present")
                .into(),
            prev: response.prev_kv.map(|x| x.into()),
        })
    }
}

pub enum LeaseIdOrHandle {
    LeaseId(LeaseId),
    LeaseHandle(LeaseHandle),
}

struct LeaseHandleRevoked;

impl LeaseIdOrHandle {
    fn get_lease_id(&self) -> Result<LeaseId, LeaseHandleRevoked> {
        match &self {
            LeaseIdOrHandle::LeaseId(lease_id) => Ok(*lease_id),
            LeaseIdOrHandle::LeaseHandle(handle) => handle.id().ok_or(LeaseHandleRevoked),
        }
    }
}

impl From<LeaseId> for LeaseIdOrHandle {
    fn from(value: LeaseId) -> Self {
        Self::LeaseId(value)
    }
}

impl From<LeaseHandle> for LeaseIdOrHandle {
    fn from(value: LeaseHandle) -> Self {
        Self::LeaseHandle(value)
    }
}

impl From<&LeaseHandle> for LeaseIdOrHandle {
    fn from(value: &LeaseHandle) -> Self {
        Self::LeaseHandle(value.clone())
    }
}

impl<TValue, TPrev> PutRequest<TValue, TPrev, ts::Unset<LeaseIdOrHandle>> {
    /// Associataes the key with a given lease, which will cause the key to be deleted when the lease
    /// is revoked.
    ///
    /// You can either provide a raw [`LeaseId`] or a [`LeaseHandle`], however,
    /// if the handle refers to a lease which is revoked, an error will be returned upon
    /// execution.
    pub fn with_lease(
        self,
        lease: impl Into<LeaseIdOrHandle>,
    ) -> PutRequest<TValue, TPrev, ts::Set<LeaseIdOrHandle>> {
        PutRequest {
            client: self.client,
            key: self.key,
            value: self.value,
            lease: ts::Set(lease.into()),
            prev: self.prev,
        }
    }

    /// Update the key using its current lease.
    ///
    /// If the key does not have a lease, then the lease is not set.
    ///
    /// Will return an error upon execution if the key does not exist.
    pub fn using_current_lease(self) -> PutRequest<TValue, TPrev, ts::Ignored<LeaseIdOrHandle>> {
        PutRequest {
            client: self.client,
            key: self.key,
            value: self.value,
            lease: ts::Ignored::new(),
            prev: self.prev,
        }
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

/// Returned when executing a [`PutRequest`] where the prev_kv is requested.
struct PutResponseWithPrevKeyValue {
    /// Set to `None` if the key was not set prior to executing the put request.
    prev_kv: Option<KeyValue>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct ClusterId(pub u64);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct MemberId(pub u64);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct RaftTerm(pub u64);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct Revision(pub i64);

pub struct Version(pub i64);

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

struct PutResponse<TPrev = ()> {
    header: ResponseHeader,
    prev: TPrev,
}

async fn qux(kv: KVClient) {
    let x = kv
        .put("hello")
        .only_updating_lease(LeaseId::new(3).unwrap())
        .return_prev_kv()
        .execute()
        .await;

    let y = kv.put("hello").with_value("world").execute().await;
}
