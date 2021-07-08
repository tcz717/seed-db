use serde::de::Visitor;
use std::convert::TryInto;

pub(crate) struct BytesVisitor<const N: usize>;
impl<'de, const N: usize> Visitor<'de> for BytesVisitor<N> {
    type Value = [u8; N];

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{} bytes expected", N)
    }
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.try_into()
            .map_err(|_| serde::de::Error::invalid_length(v.len(), &self))?)
    }
}
struct BorrowedBytesVisitor<const N: usize>;
impl<'de, const N: usize> Visitor<'de> for BorrowedBytesVisitor<N> {
    type Value = &'de [u8; N];

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "{} bytes expected", N)
    }
    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(TryInto::<&[u8; N]>::try_into(v)
            .map_err(|_| serde::de::Error::invalid_length(v.len(), &self))?)
    }
}

pub(crate) struct BytesSliceVisitor;
impl<'de> Visitor<'de> for BytesSliceVisitor {
    type Value = &'de [u8];

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "bytes expected")
    }
    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v)
    }
}
