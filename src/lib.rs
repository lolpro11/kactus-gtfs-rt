use std::time::Duration;

#[macro_use]
extern crate serde_derive;


//stores the config for each agency
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgencyInfo {
    pub onetrip: String,
    pub realtime_vehicle_positions: String,
    pub realtime_trip_updates: String,
    pub realtime_alerts: String,
    pub has_auth: bool,
    pub auth_type: String,
    pub auth_header: String,
    pub auth_password: String,
    pub fetch_interval: f32,
    pub multiauth: Option<Vec<String>>,
}

impl PartialEq for AgencyInfo {
    fn eq(&self, other: &Self) -> bool {
        self.onetrip == other.onetrip
    }
}


#[derive(Debug)]
pub struct Agencyurls {
    pub vehicles: Option<String>,
    pub trips: Option<String>,
    pub alerts: Option<String>,
}


pub async fn fetchurl(
    url: &Option<String>,
    auth_header: &String,
    auth_type: &String,
    auth_password: &String,
    client: &reqwest::Client,
    timeoutforfetch: u64,
) -> Option<Vec<u8>> {
    if url.is_none() || url.to_owned().unwrap().contains("kactus") {
        return None;
    }
    let mut req = client.get(url.to_owned().unwrap());

    if auth_type == "header" {
        req = req.header(auth_header, auth_password);
    }

    let resp = req
        .timeout(Duration::from_millis(timeoutforfetch))
        .send()
        .await;

    match resp {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.bytes().await {
                    Ok(bytes_pre) => {
                        let bytes = bytes_pre.to_vec();
                        Some(bytes)
                    }
                    _ => None,
                }
            } else {
                println!("{}:{:?}", &url.clone().unwrap(), resp.status());
                None
            }
        }
        Err(e) => {
            println!("error fetching url: {:?}", e);
            None
        }
    }
}

pub fn make_url(
    url: &String,
    auth_type: &String,
    auth_header: &String,
    auth_password: &String,
) -> Option<String> {
    if !url.is_empty() {
        let mut outputurl = url.clone();

        if !auth_password.is_empty() && auth_type == "query_param" {
            outputurl = outputurl.replace("PASSWORD", &auth_password);
        }

        return Some(outputurl);
    }
    return None;
}

pub fn parse_protobuf_message(
    bytes: &[u8],
) -> Result<gtfs_rt::FeedMessage, Box<dyn std::error::Error>> {
    let x = prost::Message::decode(bytes);

    if x.is_ok() {
        return Ok(x.unwrap());
    } else {
        return Err(Box::new(x.unwrap_err()));
    }
}

pub mod insert {

    use prost::Message;
    use redis::{Commands, Connection};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn insert_gtfs_rt_bytes(
        con: &mut Connection,
        bytes: &Vec<u8>,
        onetrip: &str,
        category: &str,
    ) {
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        let key: String = format!("gtfsrt|{}|{}", &onetrip, &category);
        let _: () = con.set(key.clone(), bytes).unwrap();

        /*let msg: Vec<u8> = bytes.clone();
        let _xadd_result: RedisResult<String> = con.xadd(
            format!("{}-{}", &onetrip, &category),
            "*",
            &[(key.clone(), msg.clone())],
        );*/
        inserttimes(con, &onetrip, &category, &now_millis);
        //let _ = con.set_read_timeout(Some(Duration::new(10, 0)));
    }
    pub fn insert_gtfs_rt(
        con: &mut Connection,
        data: &gtfs_rt::FeedMessage,
        onetrip: &str,
        category: &str,
    ) {
        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        let bytes: Vec<u8> = data.encode_to_vec();

        let _: () = con
            .set(format!("gtfsrt|{}|{}", &onetrip, &category), bytes.to_vec())
            .unwrap();

        inserttimes(con, &onetrip, &category, &now_millis);
    }

    fn inserttimes(con: &mut Connection, onetrip: &str, category: &str, now_millis: &String) {
        let _: () = con
            .set(
                format!("gtfsrttime|{}|{}", &onetrip, &category),
                &now_millis,
            )
            .unwrap();

        let _: () = con
            .set(format!("gtfsrtexists|{}", &onetrip), &now_millis)
            .unwrap();
    }
}

pub mod aspen {
    pub async fn send_to_aspen(
        agency: &str,
        vehicles_result: &Option<Vec<u8>>,
        trips_result: &Option<Vec<u8>>,
        alerts_result: &Option<Vec<u8>>,
        vehicles_exist: bool,
        trips_exist: bool,
        alerts_exist: bool,
        useexistingdata: bool,
    ) {
        //send data to aspen over tarpc
        //  let redisclient = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
        //let _con = redisclient.get_connection().unwrap();

        let generating_vehicles = [
            "f-mta~nyc~rt~mnr",
            "f-mta~nyc~rt~lirr",
            "f-roamtransit~rt",
            "f-bart~rt",
        ];

        let vehicles_exist = generating_vehicles.contains(&agency) || vehicles_exist;
    }
}
