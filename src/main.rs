mod gorilla;
mod http;
mod store;

use std::{env, sync::Arc, time::Duration};

use store::Store;

fn env_secs(name: &str, default: u64) -> Duration {
    let seconds = env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .unwrap_or_else(|_| panic!("{name} must be an unsigned integer"))
        })
        .unwrap_or(default);
    assert!(seconds > 0, "{name} must be greater than zero");
    Duration::from_secs(seconds)
}

#[tokio::main]
async fn main() {
    let address = env::var("DASH_ADDR").unwrap_or_else(|_| "127.0.0.1:9090".to_owned());
    let retention = env_secs("DASH_RETENTION_SECS", 10_800);
    let chunk_window = env_secs("DASH_CHUNK_SECS", 1_800);
    let store = Arc::new(Store::new(retention, chunk_window));

    let sweeper_store = Arc::clone(&store);
    let sweeper = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        loop {
            interval.tick().await;
            sweeper_store.sweep(http::now_ms());
        }
    });

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .unwrap_or_else(|error| panic!("failed to bind {address}: {error}"));
    println!(
        "dash listening on {address}, retention={}s, chunk_window={}s",
        retention.as_secs(),
        chunk_window.as_secs()
    );

    axum::serve(listener, http::router(store))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("server failed");
    sweeper.abort();
}
