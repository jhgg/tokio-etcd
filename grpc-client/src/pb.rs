pub mod authpb {
    tonic::include_proto!("authpb");
}

pub mod etcdserverpb {
    tonic::include_proto!("etcdserverpb");

    // This is the watch id that is returned when a progress request is made.
    pub const PROGRESS_WATCH_ID: i64 = -1;
}

pub mod mvccpb {
    tonic::include_proto!("mvccpb");
}

// TODO: Uncomment these when implementing.
// pub mod v3electionpb {
//     tonic::include_proto!("v3electionpb");
// }

// pub mod v3lockpb {
//     tonic::include_proto!("v3lockpb");
// }

impl etcdserverpb::WatchResponse {
    pub fn is_progress_notify(&self) -> bool {
        self.events.is_empty()
            && !self.canceled
            && !self.created
            && self.compact_revision == 0
            && self.header.map_or(0, |h| h.revision) != 0
    }
}
