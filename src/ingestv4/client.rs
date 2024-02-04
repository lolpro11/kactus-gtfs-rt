use anyhow::Ok;
use kactus::AgencyInfo;
use tarpc::{client, context, tokio_serde::formats::Json};


#[tarpc::service]
pub trait IngestInfo {
    async fn agencies() -> String;
    async fn addagency(agency: AgencyInfo) -> String;
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server =
        tarpc::serde_transport::tcp::connect("localhost:9010", Json::default).await?;
    let client = IngestInfoClient::new(client::Config::default(), server).spawn();

    let ctx = context::current();

    let agency_info = AgencyInfo {
        onetrip: "lol~rt".to_string(),
        realtime_vehicle_positions: "https://example.com/api/realtime/vehicle_positions".to_string(),
        realtime_trip_updates: "https://example.com/api/realtime/trip_updates".to_string(),
        realtime_alerts: "https://example.com/api/realtime/alerts".to_string(),
        has_auth: false,
        auth_type: "None".to_string(),
        auth_header: "None".to_string(),
        auth_password: "None".to_string(),
        fetch_interval: 60.0,
        multiauth: None,
    };
    println!("{:?}", client.addagency(ctx, agency_info).await?);

    println!("{:?}", client.agencies(ctx).await?);

    Ok(())
}
