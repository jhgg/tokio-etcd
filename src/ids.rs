use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchId(pub(crate) u64);

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct LeaseId(NonZeroU64);

impl std::hash::Hash for WatchId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.get().hash(state);
    }
}

impl WatchId {
    pub fn get(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Debug for LeaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("LeaseId").field(&self.0.get()).finish()
    }
}

impl LeaseId {
    pub(crate) fn new(lease_id: i64) -> Option<LeaseId> {
        NonZeroU64::new(lease_id as _).map(|l| Self(l))
    }
}

impl std::hash::Hash for LeaseId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.get().hash(state);
    }
}

impl LeaseId {
    pub fn get(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Default)]
pub(crate) struct IdFastHasherBuilder;

pub(crate) struct IdFastHasher(i64);

impl std::hash::BuildHasher for IdFastHasherBuilder {
    type Hasher = IdFastHasher;

    fn build_hasher(&self) -> Self::Hasher {
        IdFastHasher(0)
    }
}

impl std::hash::Hasher for IdFastHasher {
    fn finish(&self) -> u64 {
        self.0 as _
    }

    fn write_i64(&mut self, i: i64) {
        self.0 = i;
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i as _;
    }

    fn write(&mut self, _: &[u8]) {
        panic!("fast hasher only supports i64");
    }
}
