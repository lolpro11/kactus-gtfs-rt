use anyhow::Ok;
use tarpc::{client, context, tokio_serde::formats::Json};


#[tarpc::service]
pub trait IngestInfo {
    async fn agencies() -> String;
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server =
        tarpc::serde_transport::tcp::connect("localhost:9010", Json::default).await?;
    let client = IngestInfoClient::new(client::Config::default(), server).spawn();

    let ctx = context::current();
    println!("{:?}", client.agencies(ctx).await?);

    Ok(())
}
