use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct MetadataBroker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MetadataPartition {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct MetadataTopic {
    pub error_code: i16,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<MetadataPartition>,
}

#[derive(Debug, Clone)]
pub struct MetadataResponse {
    pub correlation_id: i32,
    pub brokers: Vec<MetadataBroker>,
    pub controller_id: i32,
    pub topics: Vec<MetadataTopic>,
}

/// Encode a Metadata v1 request.
///
/// Pass `topics: None` to request metadata for all topics.
/// Pass `topics: Some(&["topic1", "topic2"])` for specific topics.
pub fn encode_request_v1(correlation_id: i32, client_id: &str, topics: Option<&[&str]>) -> Vec<u8> {
    let client_id_bytes = client_id.as_bytes();

    // Calculate message size (everything after the 4-byte size prefix)
    let mut message_size: i32 = 2 + 2 + 4 + 2 + client_id_bytes.len() as i32 + 4;
    if let Some(topic_list) = topics {
        for t in topic_list {
            message_size += 2 + t.len() as i32;
        }
    }

    let mut buf = Vec::with_capacity((4 + message_size) as usize);

    // Size prefix
    buf.extend_from_slice(&message_size.to_be_bytes());
    // Header: api_key=3, version=1, correlation_id, client_id
    buf.extend_from_slice(&3_i16.to_be_bytes());
    buf.extend_from_slice(&1_i16.to_be_bytes());
    buf.extend_from_slice(&correlation_id.to_be_bytes());
    buf.extend_from_slice(&(client_id_bytes.len() as i16).to_be_bytes());
    buf.extend_from_slice(client_id_bytes);

    // Topics array: -1 = null (all topics), otherwise array of strings
    match topics {
        None => {
            buf.extend_from_slice(&(-1_i32).to_be_bytes());
        }
        Some(topic_list) => {
            buf.extend_from_slice(&(topic_list.len() as i32).to_be_bytes());
            for t in topic_list {
                let t_bytes = t.as_bytes();
                buf.extend_from_slice(&(t_bytes.len() as i16).to_be_bytes());
                buf.extend_from_slice(t_bytes);
            }
        }
    }

    buf
}

pub fn decode_response_v1(data: &[u8]) -> Result<MetadataResponse> {
    let mut pos = 0;

    let correlation_id = read_i32(data, &mut pos)?;

    // Brokers array
    let broker_count = read_i32(data, &mut pos)? as usize;
    let mut brokers = Vec::with_capacity(broker_count);
    for _ in 0..broker_count {
        let node_id = read_i32(data, &mut pos)?;
        let host = read_string(data, &mut pos)?;
        let port = read_i32(data, &mut pos)?;
        let rack = read_nullable_string(data, &mut pos)?;
        brokers.push(MetadataBroker { node_id, host, port, rack });
    }

    // Controller ID
    let controller_id = read_i32(data, &mut pos)?;

    // Topics array
    let topic_count = read_i32(data, &mut pos)? as usize;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let error_code = read_i16(data, &mut pos)?;
        let name = read_string(data, &mut pos)?;
        let is_internal = read_i8(data, &mut pos)? != 0;

        // Partitions array
        let partition_count = read_i32(data, &mut pos)? as usize;
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            let p_error_code = read_i16(data, &mut pos)?;
            let partition_index = read_i32(data, &mut pos)?;
            let leader_id = read_i32(data, &mut pos)?;

            let replica_count = read_i32(data, &mut pos)? as usize;
            let mut replica_nodes = Vec::with_capacity(replica_count);
            for _ in 0..replica_count {
                replica_nodes.push(read_i32(data, &mut pos)?);
            }

            let isr_count = read_i32(data, &mut pos)? as usize;
            let mut isr_nodes = Vec::with_capacity(isr_count);
            for _ in 0..isr_count {
                isr_nodes.push(read_i32(data, &mut pos)?);
            }

            partitions.push(MetadataPartition {
                error_code: p_error_code,
                partition_index,
                leader_id,
                replica_nodes,
                isr_nodes,
            });
        }

        topics.push(MetadataTopic { error_code, name, is_internal, partitions });
    }

    Ok(MetadataResponse { correlation_id, brokers, controller_id, topics })
}

fn read_i8(data: &[u8], pos: &mut usize) -> Result<i8> {
    if *pos + 1 > data.len() {
        return Err(Error::ProtocolError("unexpected end of data".into()));
    }
    let val = data[*pos] as i8;
    *pos += 1;
    Ok(val)
}

fn read_i16(data: &[u8], pos: &mut usize) -> Result<i16> {
    if *pos + 2 > data.len() {
        return Err(Error::ProtocolError("unexpected end of data".into()));
    }
    let val = i16::from_be_bytes(data[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(val)
}

fn read_i32(data: &[u8], pos: &mut usize) -> Result<i32> {
    if *pos + 4 > data.len() {
        return Err(Error::ProtocolError("unexpected end of data".into()));
    }
    let val = i32::from_be_bytes(data[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(val)
}

fn read_string(data: &[u8], pos: &mut usize) -> Result<String> {
    let len = read_i16(data, pos)? as usize;
    if *pos + len > data.len() {
        return Err(Error::ProtocolError("unexpected end of data".into()));
    }
    let s = String::from_utf8(data[*pos..*pos + len].to_vec())
        .map_err(|e| Error::ProtocolError(format!("invalid utf-8: {e}")))?;
    *pos += len;
    Ok(s)
}

fn read_nullable_string(data: &[u8], pos: &mut usize) -> Result<Option<String>> {
    let len = read_i16(data, pos)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    if *pos + len > data.len() {
        return Err(Error::ProtocolError("unexpected end of data".into()));
    }
    let s = String::from_utf8(data[*pos..*pos + len].to_vec())
        .map_err(|e| Error::ProtocolError(format!("invalid utf-8: {e}")))?;
    *pos += len;
    Ok(Some(s))
}
