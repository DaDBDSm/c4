use tokio::fs::{self, File};
use tokio::io::ErrorKind::AlreadyExists;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};

#[derive(Clone)]
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

    pub async fn create_file<S>(&self, stream: S, header: &[u8], path: &str) -> io::Result<u64>
    where
        S: Stream<Item = Vec<u8>> + Unpin,
    {
        let mut file = File::create(path).await?;
        let mut total_written: u64 = 0;

        file.write_all(header).await?;
        total_written += header.len() as u64;

        tokio::pin!(stream);

        while let Some(chunk) = stream.next().await {
            let bytes = chunk;
            let n = bytes.len();

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

            file.write_all(&bytes).await?;
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

    pub async fn stream_file(&self, path: &str) -> io::Result<ReceiverStream<io::Result<Vec<u8>>>> {
        let mut file = File::open(path).await?;
        let (tx, rx) = mpsc::channel(self.buffer_size_bytes);
        let buffer_size = self.buffer_size_bytes;

        tokio::spawn(async move {
            let mut buffer = vec![0u8; buffer_size];

            loop {
                match file.read(&mut buffer).await {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        let chunk = buffer[..n].to_vec();
                        if tx.send(Ok(chunk)).await.is_err() {
                            break; // Receiver dropped
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }
}
