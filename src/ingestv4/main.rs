use std::{collections::HashMap, fs::File, io::BufReader, sync::{Arc, Mutex}, thread::sleep, time::Duration};

use kactus::AgencyInfo;
use reqwest::Client;
use stoppable_thread;

use futures::{future, prelude::*};
use tarpc::{
    client, context, server::{incoming::Incoming, BaseChannel}, tokio_serde::formats::Json
};
use tokio::task;

#[tarpc::service]
pub trait IngestInfo {
    async fn agencies() -> String;
    async fn addagency(agency: AgencyInfo) -> String;
}

#[derive(Clone)]
struct KactusRPC {
    agencies: Arc<Mutex<Vec<AgencyInfo>>>,
}

#[tarpc::server]
impl IngestInfo for KactusRPC {
    async fn agencies(self, _: context::Context) -> String {
        serde_json::to_string(&self.agencies).expect("Failed to serialize to JSON")
    }
    async fn addagency(self, _: context::Context, agency: AgencyInfo) -> String {
        //addtolist(self, agency);
        if !self.agencies.lock().unwrap().contains(&agency) {
            self.agencies.lock().unwrap().push(agency);
            return "Agency added".to_string();
        }
        return "Error: Agency Exists".to_string();
    }
}


fn fetchagency(client: &Client, agency: AgencyInfo)  {
    //let client = reqwest::ClientBuilder::new().deflate(true).gzip(true).brotli(true).build().unwrap();
    loop {
        sleep(Duration::new(1, 0));
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let arguments = std::env::args();
    let arguments = arguments::parse(arguments).unwrap();

    let filename = match arguments.get::<String>("urls") {
        Some(filename) => filename,
        None => String::from("urls.csv"),
    };

    let timeoutforfetch = match arguments.get::<u64>("timeout") {
        Some(filename) => filename,
        None => 15_000,
    };

    let file = File::open(filename).unwrap();

    let mut reader = csv::Reader::from_reader(BufReader::new(file));

    let mut agencies: Vec<AgencyInfo> = Vec::new();

    for record in reader.records() {
        match record {
            Ok(record) => {
                let agency = AgencyInfo {
                    onetrip: record[0].to_string(),
                    realtime_vehicle_positions: record[1].to_string(),
                    realtime_trip_updates: record[2].to_string(),
                    realtime_alerts: record[3].to_string(),
                    has_auth: record[4].parse().unwrap(),
                    auth_type: record[5].to_string(),
                    auth_header: record[6].to_string(),
                    auth_password: record[7].to_string(),
                    fetch_interval: record[8].parse().unwrap(),
                    multiauth: {
                        if !record[9].to_string().is_empty() {
                            let mut outputvec: Vec<String> = Vec::new();
                            for s in record[9].to_string().clone().split(",") {
                                outputvec.push(s.to_string());
                            }
                    
                            Some(outputvec)
                        } else {
                            None
                        }
                    },
                };

                agencies.push(agency);
            }
            Err(e) => {
                println!("error reading csv");
                println!("{:?}", e);
            }
        }
    }

    let shared_client = Arc::new(reqwest::ClientBuilder::new()
        .deflate(true)
        .gzip(true)
        .brotli(true)
        .build()
        .unwrap()
    );
    let mut handles = HashMap::new();

    let base = KactusRPC {agencies: Arc::new(Mutex::new(Vec::new()))};

    let listener = tarpc::serde_transport::tcp::listen("localhost:9010", Json::default)
        .await?
        .filter_map(|r| future::ready(r.ok()));
    let server = listener
        .map(BaseChannel::with_defaults)
        .execute(base.serve());
    let j = task::spawn(server);

    for agency in agencies.into_iter() {
        let key = agency.onetrip.clone();
        let shared_client_clone = Arc::clone(&shared_client);
        let handle = stoppable_thread::spawn(move |_msg| {
            let client = shared_client_clone;
            fetchagency(&client, agency.clone());
        });

        handles.insert(key, handle);
    }

    /*let connection =
        tarpc::serde_transport::tcp::connect("localhost:9010", Json::default).await?;
    let client = IngestInfoClient::new(client::Config::default(), connection).spawn();

    let ctx = context::current();

    println!("{:?}", client.agencies(ctx).await?);*/
    for (index, handle) in handles {
        match handle.join() {
            Ok(_) => println!("Thread {} finished successfully", index),
            Err(_) => println!("Thread {} panicked", index),
        }
    }
    println!("Ready at localhost");
    j.await?;
    Ok(())
}