use tokio::task::JoinHandle;
use tokio_etcd::{
    watcher::{CoalescedWatch, Key, WatchConfig, WatchError},
    Client, ClientEndpointConfig,
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = Client::new(
        // fixme: no need for into_iter but yet here we are. :(
        vec!["localhost"].into_iter(),
        ClientEndpointConfig::http(),
    );
    let jhs = vec![
        spawn_watcher_task(1, &client, "hello"),
        spawn_watcher_task(2, &client, "world"),
        spawn_watcher_task(3, &client, "world"),
        spawn_watch_all(&client),
    ];

    for jh in jhs {
        let _ = jh.await.unwrap();
    }
}

fn spawn_watcher_task(
    i: i32,
    client: &Client,
    key: impl Into<String>,
) -> JoinHandle<Result<(), WatchError>> {
    let watcher = client.watcher();
    let key = Key::from(key.into());

    tokio::task::spawn(async move {
        let CoalescedWatch {
            value,
            mut receiver,
        } = watcher.watch_key_coalesced(key).await?;

        println!("{i}: watch state: {value:?}");

        loop {
            match receiver.recv().await {
                Ok(value) => {
                    println!("{i}: new value: {value:?}");
                }
                Err(cancelled) => {
                    println!("{i}: watch cancelled: {cancelled:?}");
                    return Ok(());
                }
            }
        }
    })
}

fn spawn_watch_all(client: &Client) -> JoinHandle<Result<(), WatchError>> {
    let watcher = client.watcher();
    tokio::task::spawn(async move {
        let mut receiver = watcher.watch_with_config(WatchConfig::for_all_keys()).await;

        loop {
            dbg!(receiver.recv().await);
        }
    })
}
