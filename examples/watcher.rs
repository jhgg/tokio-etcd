use tokio::task::JoinHandle;
use tokio_etcd::{
    watcher::{WatchError, Watched, WatcherKey},
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
    let key = WatcherKey::key_str(key);

    tokio::task::spawn(async move {
        let Watched {
            value,
            mut receiver,
        } = watcher.watch(key).await?;

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
