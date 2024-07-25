use tokio_etcd::{Client, ClientEndpointConfig, WatcherKey};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = Client::new(
        // fixme: no need for into_iter but yet here we are. :(
        vec!["localhost"].into_iter(),
        ClientEndpointConfig::http(),
    );
    let watcher = client.watcher();
    let mut wr = watcher.watch(WatcherKey::key_str("hello")).await.unwrap();

    println!("{:?}", wr);

    while let Ok(event) = wr.receiver.recv().await {
        println!("{:?}", event);
    }
}
