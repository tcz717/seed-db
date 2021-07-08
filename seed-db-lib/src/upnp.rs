use std::{
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    time::Duration,
};

use futures::prelude::*;
use rupnp::ssdp::{SearchTarget, URN};
use tokio::pin;

const WAN_IP_CONNECTION: URN = URN::service("schemas-upnp-org", "WANIPConnection", 1);

pub async fn open_forward(_port: u16) -> Option<IpAddr> {
    let search_target = SearchTarget::URN(WAN_IP_CONNECTION);
    let devices = rupnp::discover(&search_target, Duration::from_secs(3))
        .await
        .ok()?;
    pin!(devices);

    while let Some(device) = devices.try_next().await.ok()? {
        let service = device
            .find_service(&WAN_IP_CONNECTION)
            .expect("searched for InternetGatewayDevice, got something else");

        let response = service
            .action(device.url(), "GetExternalIPAddress", "")
            .await
            .ok()?;

        let ip_str = response.get("NewExternalIPAddress").unwrap();

        println!("'{}' is at ip {}", device.friendly_name(), ip_str);

        if let Ok(ipv4) = Ipv4Addr::from_str(ip_str) {
            return Some(ipv4.into());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::open_forward;

    #[tokio::test]
    async fn test_open_forward() {
        open_forward(rand::thread_rng().gen_range(10000..u16::MAX))
            .await
            .unwrap();
    }
}
