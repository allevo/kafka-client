use crate::error::{Error, Result};

pub fn encode_request_v1(correlation_id: i32, client_id: &str, mechanism: &str) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let mechanism_bytes = mechanism.as_bytes();
    let message_size: i32 =
        (2 + 2 + 4 + 2 + client_id_bytes.len() + 2 + mechanism_bytes.len()) as i32;

    let mut buf = Vec::with_capacity((4 + message_size) as usize);
    buf.extend_from_slice(&message_size.to_be_bytes());
    buf.extend_from_slice(&17_i16.to_be_bytes()); // api_key
    buf.extend_from_slice(&1_i16.to_be_bytes()); // api_version
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    buf.extend_from_slice(&(client_id_bytes.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id_bytes);
    buf.extend_from_slice(&(mechanism_bytes.len() as i16).to_be_bytes());
    buf.extend_from_slice(mechanism_bytes);
    buf
}

pub fn decode_response_v1(data: &[u8]) -> Result<(i32, Vec<String>)> {
    if data.len() < 10 {
        return Err(Error::ProtocolError("sasl handshake response too short".into()));
    }

    let correlation_id = i32::from_be_bytes(data[0..4].try_into().unwrap());
    let error_code = i16::from_be_bytes(data[4..6].try_into().unwrap());

    let mechanism_count = i32::from_be_bytes(data[6..10].try_into().unwrap()) as usize;
    let mut offset = 10;
    let mut mechanisms = Vec::with_capacity(mechanism_count);
    for _ in 0..mechanism_count {
        if offset + 2 > data.len() {
            return Err(Error::ProtocolError("sasl handshake response truncated".into()));
        }
        let len = i16::from_be_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if offset + len > data.len() {
            return Err(Error::ProtocolError("sasl handshake response truncated".into()));
        }
        mechanisms.push(
            String::from_utf8(data[offset..offset + len].to_vec())
                .map_err(|_| Error::ProtocolError("invalid mechanism name".into()))?,
        );
        offset += len;
    }

    if error_code != 0 {
        return Err(Error::AuthenticationError(format!(
            "sasl handshake failed with error code {error_code}, broker supports: {mechanisms:?}"
        )));
    }

    Ok((correlation_id, mechanisms))
}
