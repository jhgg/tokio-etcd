use std::time::Duration;

use tokio_etcd::{Client, ClientEndpointConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = Client::new(
        // fixme: no need for into_iter but yet here we are. :(
        vec!["localhost"].into_iter(),
        ClientEndpointConfig::http(),
    );

    let lease = client.grant_lease(Duration::from_secs(10)).await.unwrap();
    println!("got lease {:?}", lease.id());

    drop(lease);

    // ...?
}
