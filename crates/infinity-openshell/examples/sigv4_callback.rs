//! SigV4 callback signing, verified by a local receiver.
//!
//! ```shell
//! cargo run -p infinity-openshell --example sigv4_callback --features sigv4
//! ```

use std::time::Duration;

use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use axum::Router;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use infinity_openshell::{CallbackClient, SigV4CallbackClient};
use rap_protocol::{ProviderCallId, RapCallback, RapToolResult, ThreadId, ToolCallId};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new().route(
        "/callback",
        post(move |headers: HeaderMap| {
            let sender = sender.clone();
            async move {
                let authorization = headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                let _ = sender.send(authorization);
                StatusCode::OK
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let provider = SharedCredentialsProvider::new(Credentials::new(
        "AKIDEXAMPLE",
        "secret",
        None,
        None,
        "example",
    ));
    let client = SigV4CallbackClient::new(
        provider,
        "us-east-1",
        "lambda",
        Vec::new(),
        false,
        Duration::from_secs(5),
    )?;

    let result = RapToolResult {
        group_id: ThreadId::from("thread-1"),
        id: ToolCallId::from("call-1"),
        call_id: None::<ProviderCallId>,
        text: Some("signed".to_owned()),
        content: None,
        display_as: None,
        subscription: None,
    };
    client
        .post(
            &format!("http://127.0.0.1:{port}/callback"),
            &RapCallback::ToolResult(result),
        )
        .await?;

    let authorization = receiver.recv().await.ok_or("no callback received")?;
    println!("authorization: {authorization}");
    assert!(authorization.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"));
    assert!(authorization.contains("/us-east-1/lambda/aws4_request"));
    println!("signature region and service are correct");
    Ok(())
}
