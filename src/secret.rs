use zeroize::Zeroizing;

/// A string that is zeroized on drop.
///
/// Wraps the inner value in [`zeroize::Zeroizing`] so the memory is
/// overwritten with zeros when the value is dropped. The secret can
/// only be accessed via [`SecretString::expose_secret`], making
/// accidental logging or display unlikely.
pub struct SecretString {
    inner: Zeroizing<String>,
}

impl SecretString {
    pub fn new(value: String) -> Self {
        Self {
            inner: Zeroizing::new(value),
        }
    }

    pub fn expose_secret(&self) -> &str {
        &self.inner
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}
