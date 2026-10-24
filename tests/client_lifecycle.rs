//! Bounded modern discovery and cancellation of owned transports.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    Client, ClientError, Server,
    protocol::{
        RoleClient, ServiceExt,
        service::{RxJsonRpcMessage, TxJsonRpcMessage},
        transport::Transport,
    },
};
use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, BufReader};

struct Stalled {
    stall_send: bool,
    dropped: Arc<AtomicBool>,
    sends: Arc<AtomicUsize>,
}
impl Drop for Stalled {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl Transport<RoleClient> for Stalled {
    type Error = std::io::Error;
    fn send(
        &mut self,
        _: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.sends.fetch_add(1, Ordering::SeqCst);
        let stall = self.stall_send;
        async move { if stall { pending().await } else { Ok(()) } }
    }
    async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        pending().await
    }
    async fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
fn stalled(stall_send: bool) -> (Stalled, Arc<AtomicBool>, Arc<AtomicUsize>) {
    let dropped = Arc::new(AtomicBool::new(false));
    let sends = Arc::new(AtomicUsize::new(0));
    (
        Stalled {
            stall_send,
            dropped: dropped.clone(),
            sends: sends.clone(),
        },
        dropped,
        sends,
    )
}
#[tokio::test(start_paused = true)]
async fn discovery_bounds_both_stalled_sends_and_responses_and_drops_transport() {
    for stall_send in [true, false] {
        let (transport, dropped, sends) = stalled(stall_send);
        let start = tokio::time::Instant::now();
        let error = Client::connect_with_timeout(transport, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(matches!(error, ClientError::DiscoveryTimeout));
        assert_eq!(start.elapsed(), Duration::from_secs(2));
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(sends.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn default_is_ten_seconds_and_zero_sends_nothing() {
    let (transport, dropped, _) = stalled(false);
    let start = tokio::time::Instant::now();
    assert!(matches!(
        Client::connect(transport).await,
        Err(ClientError::DiscoveryTimeout)
    ));
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    assert!(dropped.load(Ordering::SeqCst));
    let (transport, dropped, sends) = stalled(false);
    assert!(matches!(
        Client::connect_with_timeout(transport, Duration::ZERO).await,
        Err(ClientError::DiscoveryTimeout)
    ));
    assert_eq!(sends.load(Ordering::SeqCst), 0);
    assert!(dropped.load(Ordering::SeqCst));
}
#[tokio::test(start_paused = true)]
async fn silent_peer_receives_only_discovery_then_eof() {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let peer = tokio::spawn(async move {
        let mut lines = BufReader::new(server_io).lines();
        let request: serde_json::Value =
            serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(request["method"], "server/discover");
        assert!(lines.next_line().await.unwrap().is_none());
    });
    assert!(matches!(
        Client::connect_with_timeout(client_io, Duration::from_secs(1)).await,
        Err(ClientError::DiscoveryTimeout)
    ));
    peer.await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn successful_discovery_does_not_leave_a_connection_deadline() {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let peer = tokio::spawn(async move {
        Server::new("deadline", "1")
            .serve(server_io)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let client = Client::connect_with_timeout(client_io, Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(20)).await;
    client.peer().list_tools(None).await.unwrap();
    client.close().await.unwrap();
    peer.await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn aborting_discovery_drops_the_transport() {
    let (transport, dropped, sends) = stalled(false);
    let connection = tokio::spawn(Client::connect_with_timeout(
        transport,
        Duration::from_secs(60),
    ));
    tokio::task::yield_now().await;
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    connection.abort();
    assert!(connection.await.unwrap_err().is_cancelled());
    assert!(dropped.load(Ordering::SeqCst));
}
