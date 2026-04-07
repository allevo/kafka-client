pub mod api_versions;
pub mod metadata;
pub mod sasl_authenticate;
pub mod sasl_handshake;

#[derive(Debug, Clone)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}
