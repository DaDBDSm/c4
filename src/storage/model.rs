use std::time::{SystemTime, UNIX_EPOCH};

pub const OBJECT_FILE_MAGIC: u32 = 0xABCABC123;

pub struct ObjectFileHeader {
    pub magic: u32,
    pub create_timestamp: i64,
}
impl ObjectFileHeader {
    const SIZE: usize = 12;

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut result = [0u8; Self::SIZE];

        // Copy magic (u32 = 4 bytes) to positions 0-3
        result[0..4].copy_from_slice(&self.magic.to_le_bytes());

        // Copy timestamp (u64 = 8 bytes) to positions 4-11
        result[4..12].copy_from_slice(&self.create_timestamp.to_le_bytes());

        result
    }

    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            create_timestamp: i64::from_le_bytes(bytes[4..12].try_into().unwrap()),
        }
    }
}
