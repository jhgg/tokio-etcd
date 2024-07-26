use tokio::task::JoinHandle;
use tokio_etcd::{Client, ClientEndpointConfig, Watcher, WatcherKey};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = Client::new(
        // fixme: no need for into_iter but yet here we are. :(
        // fixme: should we let user specify port?
        vec!["localhost"].into_iter(),
        ClientEndpointConfig::http(),
    );
    let watcher = client.watcher();
    let jhs = vec![
        spawn_watcher_task(1, &watcher, "hello"),
        spawn_watcher_task(2, &watcher, "world"),
        spawn_watcher_task(3, &watcher, "world"),
    ];

    for jh in jhs {
        jh.await.unwrap();
    }
}

fn spawn_watcher_task(i: i32, watcher: &Watcher, key: impl Into<String>) -> JoinHandle<()> {
    let watcher = watcher.clone();
    let key = WatcherKey::key_str(key);

    tokio::task::spawn(async move {
        let mut wr = watcher.watch(key).await.unwrap();

        println!("{i}: watch state: {wr:?}");

        while let Ok(value) = wr.receiver.recv().await {
            println!("{i}: new value: {value:?}");
        }
    })
}
