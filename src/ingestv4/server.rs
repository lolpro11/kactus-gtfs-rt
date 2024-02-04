use anyhow::Ok;
use futures::{future, prelude::*};
use tarpc::{
    client, context, server::{incoming::Incoming, BaseChannel}, tokio_serde::formats::Json
};
use tokio::task;

#[tarpc::service]
pub trait World {
    async fn hello(name: String) -> String;
}

#[derive(Clone)]
struct HelloServer;

#[tarpc::server]
impl World for HelloServer {
    async fn hello(self, _: context::Context, name: String) -> String {
        format!("Hello, {name}!")
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = tarpc::serde_transport::tcp::listen("localhost:9010", Json::default)
        .await?
        .filter_map(|r| future::ready(r.ok()));
    let server = listener
        .map(BaseChannel::with_defaults)
        .execute(HelloServer.serve());
    let j = task::spawn(server);

    let connection =
        tarpc::serde_transport::tcp::connect("localhost:9010", Json::default).await?;
    let client = WorldClient::new(client::Config::default(), connection).spawn();

    let ctx = context::current();
    println!("{:?}", client.hello(ctx, "lol".to_string()).await?);
    

    j.await?;

    Ok(())
}
