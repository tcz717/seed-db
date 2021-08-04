pub mod crawler;
pub mod dht;
pub mod peer;
pub mod route;
#[cfg(feature = "nat_traversal")]
pub mod upnp;
mod utils;

pub fn parse_magnet(url: &str) -> Option<dht::InfoHash> {
    let encoded = url
        .split('&')
        .find_map(|q| q.strip_prefix("magnet:?xt=urn:btih:"))?;

    use hex::FromHex;
    use std::convert::TryInto;
    match encoded.len() {
        40 => Some(dht::InfoHash::from_hex(encoded).ok()?),
        28 => Some(base64::decode(encoded).ok()?.as_slice().try_into().ok()?),
        _ => None,
    }
}
