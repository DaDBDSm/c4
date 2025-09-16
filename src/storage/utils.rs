use std::io::{Cursor, Read};

pub fn add_prefix_to_reader<R: Read>(prefix: &[u8], reader: R) -> impl Read {
    Cursor::new(prefix).chain(reader)
}
