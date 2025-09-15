use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
};

const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024; // 10 MB limit
const BUFFER_SIZE: usize = 8192;

pub fn create_file<R: Read>(reader: &mut R, path: &str) -> io::Result<()> {
    let mut file = File::create(path)?;
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut total_written: u64 = 0;

    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        total_written += n as u64;

        if total_written > MAX_FILE_SIZE {
            // delete the partially written file
            drop(file);
            let _ = fs::remove_file(path);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File exceeds maximum allowed size",
            ));
        }

        file.write_all(&buffer[..n])?;
    }

    Ok(())
}

pub fn delete_file(path: &str) -> io::Result<()> {
    if Path::new(path).exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub fn get_file_reader(path: &str) -> io::Result<impl Read> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;

    if metadata.len() > MAX_FILE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "File exceeds maximum allowed size",
        ));
    }

    Ok(file)
}

pub fn create_dir(path: &str) -> io::Result<()> {
    fs::create_dir_all(path)?;
    Ok(())
}

pub fn delete_dir(path: &str) -> io::Result<()> {
    if Path::new(path).exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

pub fn list_dir(path: &str) -> io::Result<Vec<String>> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name().into_string().unwrap_or_default();
        entries.push(name);
    }
    Ok(entries)
}
