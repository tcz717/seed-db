use std::collections::HashMap;

use bytes::BytesMut;

use tokio::io;

use tokio_util::codec::Decoder;

pub struct BencodeConverter;

impl Decoder for BencodeConverter {
    type Item = BencodedData;
    type Error = io::Error;
    fn decode(
        &mut self,
        _: &mut BytesMut,
    ) -> Result<Option<<Self as Decoder>::Item>, <Self as Decoder>::Error> {
        todo!()
    }
}

pub enum BencodedData {
    Dictionary(BencodedDictionary)
}

pub struct BencodedDictionary(HashMap<BencodedData, BencodedData>);