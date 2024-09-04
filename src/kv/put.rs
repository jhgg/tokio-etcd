use crate::{ids::LeaseId, lease::LeaseHandle, watcher::Key};

use super::{ts, KeyValue, ResponseHeader};

use thiserror::Error;
use tokio_etcd_grpc_client::{self as pb};
use tonic::Status;

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
    pub(super) fn new(client: pb::KvClient<pb::AuthedChannel>, key: Key) -> Self {
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
pub enum PutError {
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
        use ts::OptionOrIgnored;

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

pub struct PutResponse<TPrev = ()> {
    pub header: ResponseHeader,
    prev: TPrev,
}

impl PutResponse<Option<KeyValue>> {
    pub fn into_prev_kv(self) -> Option<KeyValue> {
        self.prev
    }
}
