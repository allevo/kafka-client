use crate::error::{Error, Result};

pub fn encode_request_v0(correlation_id: i32, client_id: &str, auth_bytes: &[u8]) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();
    let message_size: i32 =
        (2 + 2 + 4 + 2 + client_id_bytes.len() + 4 + auth_bytes.len()) as i32;

    let mut buf = Vec::with_capacity((4 + message_size) as usize);
    buf.extend_from_slice(&message_size.to_be_bytes());
    buf.extend_from_slice(&36_i16.to_be_bytes()); // api_key
    buf.extend_from_slice(&0_i16.to_be_bytes()); // api_version
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    buf.extend_from_slice(&(client_id_bytes.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id_bytes);
    buf.extend_from_slice(&(auth_bytes.len() as i32).to_be_bytes());
    buf.extend_from_slice(auth_bytes);
    buf
}

pub fn decode_response_v0(data: &[u8]) -> Result<i32> {
    if data.len() < 6 {
        return Err(Error::ProtocolError(
            "sasl authenticate response too short".into(),
        ));
    }

    let correlation_id = i32::from_be_bytes(data[0..4].try_into().unwrap());
    let error_code = i16::from_be_bytes(data[4..6].try_into().unwrap());

    if error_code != 0 {
        let error_message = if data.len() >= 8 {
            let msg_len = i16::from_be_bytes(data[6..8].try_into().unwrap());
            if msg_len >= 0 {
                let len = msg_len as usize;
                if data.len() >= 8 + len {
                    String::from_utf8_lossy(&data[8..8 + len]).into_owned()
                } else {
                    "unknown error".into()
                }
            } else {
                "unknown error".into()
            }
        } else {
            "unknown error".into()
        };
        return Err(Error::AuthenticationError(error_message));
    }

    Ok(correlation_id)
}
