use crate::error::{Error, Result};
use crate::protocol::ApiVersion;

pub fn encode_request_v0(correlation_id: i32, client_id: &str) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let message_size: i32 = (2 + 2 + 4 + 2 + client_id_bytes.len()) as i32;

    let mut buf = Vec::with_capacity((4 + message_size) as usize);
    buf.extend_from_slice(&message_size.to_be_bytes());
    buf.extend_from_slice(&18_i16.to_be_bytes());
    buf.extend_from_slice(&0_i16.to_be_bytes());
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    buf.extend_from_slice(&(client_id_bytes.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id_bytes);
    buf
}

pub fn decode_response_v0(data: &[u8]) -> Result<(i32, Vec<ApiVersion>)> {
    if data.len() < 10 {
        return Err(Error::ProtocolError("response too short".into()));
    }

    let correlation_id = i32::from_be_bytes(data[0..4].try_into().unwrap());
    let error_code = i16::from_be_bytes(data[4..6].try_into().unwrap());
    if error_code != 0 {
        return Err(Error::ApiError { error_code });
    }

    let api_count = i32::from_be_bytes(data[6..10].try_into().unwrap()) as usize;
    let expected = 10 + api_count * 6;
    if data.len() < expected {
        return Err(Error::ProtocolError(format!(
            "response truncated: expected {expected} bytes, got {}",
            data.len()
        )));
    }

    let mut versions = Vec::with_capacity(api_count);
    for i in 0..api_count {
        let offset = 10 + i * 6;
        versions.push(ApiVersion {
            api_key: i16::from_be_bytes(data[offset..offset + 2].try_into().unwrap()),
            min_version: i16::from_be_bytes(data[offset + 2..offset + 4].try_into().unwrap()),
            max_version: i16::from_be_bytes(data[offset + 4..offset + 6].try_into().unwrap()),
        });
    }

    Ok((correlation_id, versions))
}
