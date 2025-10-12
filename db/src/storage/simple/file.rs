use std::io::Cursor;
use tokio::fs::{self, File};
use tokio::io::ErrorKind::AlreadyExists;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWriteExt};

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

    pub async fn create_file<R: AsyncRead + Unpin>(
        &self,
        reader: &mut R,
        path: &str,
    ) -> io::Result<u64> {
        let mut file = File::create(path).await?;

        let mut buffer = vec![0u8; self.buffer_size_bytes];
        let mut total_written: u64 = 0;

        loop {
            let n = reader.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            total_written += n as u64;

            if total_written > self.max_file_size_bytes {
                fs::remove_file(path).await?;
                return Err(io::Error::new(
                    io::ErrorKind::FileTooLarge,
                    format!(
                        "File exceeds maximum allowed size ({} MB)",
                        self.max_file_size_bytes / 1024 / 1024,
                    ),
                ));
            }

            file.write(&buffer[..n]).await?;
        }

        Ok(total_written)
    }

    pub async fn delete_file(&self, path: &str) -> io::Result<()> {
        if fs::try_exists(path).await? {
            fs::remove_file(path).await?;
        }
        Ok(())
    }

    pub async fn create_dir(&self, path: &str) -> io::Result<()> {
        if fs::try_exists(path).await? {
            return Err(io::Error::new(AlreadyExists, "Directory already exists"));
        }
        fs::create_dir_all(path).await?;
        Ok(())
    }

    pub async fn delete_dir(&self, path: &str) -> io::Result<()> {
        if fs::try_exists(path).await? {
            fs::remove_dir_all(path).await?;
        }
        Ok(())
    }

    pub async fn list_dir(&self, path: &str) -> io::Result<Vec<String>> {
        let mut result = Vec::new();
        let mut entries = fs::read_dir(path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().into_string().unwrap_or_default();
            result.push(name);
        }
        Ok(result)
    }

    pub async fn open_file_checked(&self, path: &str) -> io::Result<File> {
        let file = File::open(path).await?;
        Ok(file)
    }

    pub fn add_prefix_to_reader<R: AsyncRead>(&self, prefix: &[u8], reader: R) -> impl AsyncRead {
        Cursor::new(prefix.to_vec()).chain(reader)
    }
}
