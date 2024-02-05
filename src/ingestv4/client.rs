use anyhow::Ok;
use kactus::{parse_protobuf_message, AgencyInfo, FeedType, IngestInfoClient};
use tarpc::{client, context, tokio_serde::formats::Json};


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server =
        tarpc::serde_transport::tcp::connect("localhost:9010", Json::default).await?;
    let client = IngestInfoClient::new(client::Config::default(), server).spawn();

    let ctx = context::current();

    let agency_info = AgencyInfo {
        onetrip: "f-bigbluebus~rt".to_string(),
        realtime_vehicle_positions: "http://gtfs.bigbluebus.com/vehiclepositions.bin".to_string(),
        realtime_trip_updates: "http://gtfs.bigbluebus.com/tripupdates.bin".to_string(),
        realtime_alerts: "http://gtfs.bigbluebus.com/alerts.bin".to_string(),
        has_auth: false,
        auth_type: "".to_string(),
        auth_header: "".to_string(),
        auth_password: "".to_string(),
        fetch_interval: 1.0,
        multiauth: None,
    };

    println!("{:?}", client.addagency(ctx, agency_info).await?);

    println!("{:?}", client.agencies(ctx).await?);



    println!("{:?}", client.removeagency(ctx, "f-bigbluebus~rt".to_string()).await?);

    println!("{:#?}", &client.getagency(ctx, "f-bigbluebus~rt".to_string(), FeedType::Alerts).await.unwrap());

    println!("{:#?}", parse_protobuf_message(&client.getagency(ctx, "f-bigbluebus~rt".to_string(), FeedType::Alerts).await.unwrap()));

    Ok(())
}
