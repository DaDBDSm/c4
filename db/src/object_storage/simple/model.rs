pub const OBJECT_FILE_MAGIC: u32 = 0xABCA;

pub struct ObjectFileHeader {
    pub magic: u32,
    pub created_at: i64,
}

impl ObjectFileHeader {
    pub const SIZE: usize = 12;

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut result = [0u8; Self::SIZE];

        result[0..4].copy_from_slice(&self.magic.to_le_bytes());

        result[4..12].copy_from_slice(&self.created_at.to_le_bytes());

        result
    }

    pub fn from_bytes(bytes: &[u8; Self::SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            created_at: i64::from_le_bytes(bytes[4..12].try_into().unwrap()),
        }
    }
}
