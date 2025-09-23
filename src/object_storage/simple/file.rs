use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::io::ErrorKind::AlreadyExists;
use std::io::{Cursor, Seek, SeekFrom};
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
};

pub struct FileManager {
    max_file_size_bytes: u64,
    buffer_size_bytes: usize,
}

impl FileManager {
    pub fn new(max_file_size_bytes: u64, buffer_size_bytes: usize) -> Self {
        FileManager {
            max_file_size_bytes,
            buffer_size_bytes,
        }
    }

    pub fn create_file<R: Read>(&self, reader: &mut R, path: &str) -> io::Result<u64> {
        let mut _file_locker = FileLocker::new(File::create(path)?);
        _file_locker.lock()?;

        let mut buffer = vec![0u8; self.buffer_size_bytes];
        let mut total_written: u64 = 0;
        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            total_written += n as u64;

            if total_written > self.max_file_size_bytes {
                _ = fs::remove_file(path);
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    format!(
                        "File exceeds maximum allowed size ({} MB)",
                        self.max_file_size_bytes / 1024 / 1024,
                    ),
                ));
            }

            _file_locker.file.write_all(&buffer[..n])?;
        }
        Ok(total_written)
    }

    pub fn delete_file(&self, path: &str) -> io::Result<()> {
        if Path::new(path).exists() {
            let mut _file_locker = FileLocker::new(File::options().write(true).open(path)?);
            _file_locker.lock()?;
            fs::remove_file(path)?;
        }
        Ok(())
    }

    pub fn create_dir(&self, path: &str) -> io::Result<()> {
        if Path::new(path).exists() {
            return Err(io::Error::new(AlreadyExists, "Directory already exists"));
        }
        fs::create_dir_all(path)?;
        Ok(())
    }

    pub fn delete_dir(&self, path: &str) -> io::Result<()> {
        if Path::new(path).exists() {
            let mut _file_locker = FileLocker::new(File::open(path)?);
            _file_locker.lock()?;
            fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    pub fn list_dir(&self, path: &str) -> io::Result<Vec<String>> {
        let mut entries = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name().into_string().unwrap_or_default();
            entries.push(name);
        }
        Ok(entries)
    }

    pub fn open_file_checked(&self, path: &str) -> io::Result<File> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if metadata.len() > self.max_file_size_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File exceeds maximum allowed size",
            ));
        }
        Ok(file)
    }

    pub fn compute_sha256_reader<R: Read>(&self, mut reader: R) -> io::Result<String> {
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; self.buffer_size_bytes];
        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }
        let digest = hasher.finalize();
        Ok(hex::encode(digest))
    }

    pub fn compute_sha256_file_payload(
        &self,
        path: &str,
        header_size: usize,
    ) -> io::Result<String> {
        let mut file = self.open_file_checked(path)?;
        file.seek(SeekFrom::Start(header_size as u64))?;
        self.compute_sha256_reader(&mut file)
    }

    pub fn add_prefix_to_reader<R: Read>(&self, prefix: &[u8], reader: R) -> impl Read {
        Cursor::new(prefix).chain(reader)
    }
}

struct FileLocker {
    file: File,
    locked: bool,
}

impl FileLocker {
    fn new(file: File) -> Self {
        FileLocker {
            file,
            locked: false,
        }
    }

    fn lock(&mut self) -> io::Result<()> {
        self.file.lock_exclusive()?;
        self.locked = true;
        Ok(())
    }

    fn unlock_file(&mut self) {
        _ = self.file.unlock();
        self.locked = false;
    }
}

impl Drop for FileLocker {
    fn drop(&mut self) {
        if self.locked {
            self.unlock_file();
        };
    }
}
