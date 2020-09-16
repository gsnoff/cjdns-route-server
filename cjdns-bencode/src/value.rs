//! Generic Bencode value.

use bendy::{decoding::FromBencode, encoding::ToBencode};
pub use bendy::decoding::Error as BdecodeError;
pub use bendy::encoding::Error as BencodeError;
use bendy::value::Value as BendyValue;

/// Generic Bencode value.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct BValue(BendyValue<'static>);

impl BValue {
    /// Create `BValue` from bencoded data bytes.
    pub fn decode(data: &[u8]) -> Result<Self, BdecodeError> {
        let v = BendyValue::from_bencode(data)?;
        Ok(BValue(v))
    }

    /// Encode this `BValue` as bencoded data bytes.
    pub fn encode(&self) -> Result<Vec<u8>, BencodeError> {
        let BValue(v) = self;
        v.to_bencode()
    }

    /// Access stored Integer value.
    pub fn as_int(&self) -> Result<i64, ()> {
        match self {
            &BValue(BendyValue::Integer(value)) => Ok(value),
            _ => Err(()),
        }
    }

    /// Access stored bytes value as UTF-8 string.
    pub fn as_string(&self) -> Result<String, ()> {
        match self {
            &BValue(BendyValue::Bytes(ref value)) => Ok(String::from_utf8(value.to_vec()).map_err(|_| ())?),
            _ => Err(()),
        }
    }

    /// Access stored bytes value.
    pub fn as_bytes(&self) -> Result<Vec<u8>, ()> {
        match self {
            &BValue(BendyValue::Bytes(ref value)) => Ok(value.to_vec()),
            _ => Err(()),
        }
    }

    /// Access stored Dict value by key and return the data under that key.
    pub fn get_dict_value(&self, key: &str) -> Result<Option<BValue>, ()> {
        let dict = match self {
            &BValue(BendyValue::Dict(ref value)) => value,
            _ => return Err(()),
        };
        let value = dict.get(key.as_bytes());
        Ok(value.cloned().map(|v| BValue(v)))
    }
}