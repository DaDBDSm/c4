use std::{
    collections::HashMap,
    io::SeekFrom,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{RwLock, mpsc},
};
use tokio_stream::StreamExt;
use tokio_stream::{Stream, wrappers::ReceiverStream};

/// Metadata for a stored chunk (per-partition)
#[derive(Debug, Clone)]
struct ChunkMeta {
    offset: u64,
    length: u64,
    deleted: bool,
    created_at: i64,
}

/// Partitioned, asynchronous, thread-safe bytes storage.
/// Each partition is stored in a separate file `part_{n}.bin`.
#[derive(Clone)]
pub struct PartitionedBytesStorage {
    base_dir: PathBuf,
    partition_count: u32,

    indexes: Vec<Arc<RwLock<HashMap<u64, ChunkMeta>>>>,

    partition_locks: Vec<Arc<RwLock<()>>>,
}

impl PartitionedBytesStorage {
    pub fn new(base_dir: PathBuf, partition_count: u32) -> Self {
        assert!(partition_count > 0, "partition_count must be > 0");

        let mut indexes = Vec::with_capacity(partition_count as usize);
        let mut partition_locks = Vec::with_capacity(partition_count as usize);
        for _ in 0..partition_count {
            indexes.push(Arc::new(RwLock::new(HashMap::new())));
            partition_locks.push(Arc::new(RwLock::new(())));
        }

        Self {
            base_dir,
            partition_count,
            indexes,
            partition_locks,
        }
    }

    fn partition_for(&self, chunk_id: u64) -> u32 {
        (chunk_id % self.partition_count as u64) as u32
    }

    pub fn file_path(&self, partition: u32) -> PathBuf {
        self.base_dir.join(format!("part_{partition}.bin"))
    }

    fn current_timestamp() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    pub async fn save_chunk<T>(
        &self,
        mut chunk_stream: T,
        chunk_id: u64,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>>
    where
        T: Stream<Item = Vec<u8>> + Unpin + Send + 'static,
    {
        let partition = self.partition_for(chunk_id);
        let part_idx = partition as usize;
        let part_lock = self.partition_locks[part_idx].clone();
        let index_lock = self.indexes[part_idx].clone();

        fs::create_dir_all(&self.base_dir).await?;

        let _part_guard = part_lock.write().await;

        let path = self.file_path(partition);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await?;

        let offset = file.seek(SeekFrom::End(0)).await?;
        let mut total_len: u64 = 0;

        while let Some(frame) = chunk_stream.next().await {
            let n = frame.len() as u64;
            if n > 0 {
                file.write_all(&frame).await?;
                total_len = total_len.wrapping_add(n);
            }
        }
        file.flush().await?;

        {
            let mut idx = index_lock.write().await;
            idx.insert(
                chunk_id,
                ChunkMeta {
                    offset,
                    length: total_len,
                    deleted: false,
                    created_at: Self::current_timestamp(),
                },
            );
        }

        Ok(total_len)
    }

    pub async fn get_chunk_metadata(
        &self,
        chunk_id: u64,
    ) -> Result<ChunkMetadata, Box<dyn std::error::Error + Send + Sync>> {
        let partition = self.partition_for(chunk_id);
        let part_idx = partition as usize;
        let index_lock = self.indexes[part_idx].clone();

        let meta = {
            let idx = index_lock.read().await;
            idx.get(&chunk_id).cloned()
        };

        let meta = match meta {
            Some(m) => m,
            None => return Err(format!("chunk {} not found", chunk_id).into()),
        };

        if meta.deleted {
            return Err(format!("chunk {} deleted", chunk_id).into());
        }

        Ok(ChunkMetadata {
            chunk_id,
            offset: meta.offset,
            size: meta.length,
            created_at: meta.created_at,
            partition,
        })
    }

    pub async fn get_chunk(
        &self,
        chunk_id: u64,
    ) -> Result<ChunkWithMetadata, Box<dyn std::error::Error + Send + Sync>> {
        let metadata = self.get_chunk_metadata(chunk_id).await?;

        let partition = self.partition_for(chunk_id);
        let part_idx = partition as usize;
        let part_lock = self.partition_locks[part_idx].clone();

        let _part_read_guard = part_lock.read().await;

        let path = self.file_path(partition);
        let mut file = File::open(&path).await?;

        file.seek(SeekFrom::Start(metadata.offset)).await?;

        let (tx, rx) = mpsc::channel::<Vec<u8>>(2);
        let length = metadata.size;

        tokio::spawn(async move {
            let mut remaining = length;
            let mut buf = vec![0u8; 8 * 1024];
            while remaining > 0 {
                let to_read = std::cmp::min(remaining, buf.len() as u64) as usize;
                match file.read_exact(&mut buf[..to_read]).await {
                    Ok(_) => {
                        if tx.send(buf[..to_read].to_vec()).await.is_err() {
                            break;
                        }
                        remaining -= to_read as u64;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx);
        Ok(ChunkWithMetadata {
            metadata,
            stream: Box::new(stream),
        })
    }

    pub async fn delete_chunk(
        &self,
        chunk_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let partition = self.partition_for(chunk_id);
        let part_idx = partition as usize;
        let index_lock = self.indexes[part_idx].clone();

        let mut idx = index_lock.write().await;
        if let Some(meta) = idx.get_mut(&chunk_id) {
            meta.deleted = true;
        }
        Ok(())
    }

    pub async fn gc_partition(
        &self,
        partition: u32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if partition >= self.partition_count {
            return Err("invalid partition".into());
        }
        let part_idx = partition as usize;
        let part_lock = self.partition_locks[part_idx].clone();
        let index_lock = self.indexes[part_idx].clone();

        let _part_guard = part_lock.write().await;

        let path = self.file_path(partition);
        if !Path::new(&path).exists() {
            return Ok(());
        }

        let mut entries: Vec<(u64, ChunkMeta)> = {
            let idx = index_lock.read().await;
            idx.iter().map(|(id, m)| (*id, m.clone())).collect()
        };

        entries.sort_by_key(|(_, m)| m.offset);

        let old_file = File::open(&path).await?;
        let mut old_file = old_file;
        let tmp_path = self.base_dir.join(format!("part_{partition}.bin.tmp"));
        let mut new_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;

        new_file.set_len(0).await?;
        new_file.seek(SeekFrom::Start(0)).await?;
        old_file.seek(SeekFrom::Start(0)).await?;

        let entries: Vec<(u64, ChunkMeta)> = {
            let idx = index_lock.read().await;
            let mut v: Vec<_> = idx.iter().map(|(id, m)| (*id, m.clone())).collect();
            v.sort_by_key(|(_, m)| m.offset);
            v
        };

        let mut new_index_map = HashMap::new();
        let mut current_new_offset: u64 = 0;

        for (id, meta) in entries.into_iter() {
            if meta.deleted {
                continue;
            }
            if meta.length == 0 {
                new_index_map.insert(
                    id,
                    ChunkMeta {
                        offset: current_new_offset,
                        length: 0,
                        deleted: false,
                        created_at: meta.created_at,
                    },
                );
                continue;
            }

            old_file.seek(SeekFrom::Start(meta.offset)).await?;
            let mut remaining = meta.length;
            let mut buffer = vec![0u8; 8 * 1024];

            let entry_start = current_new_offset;

            while remaining > 0 {
                let to_read = std::cmp::min(remaining, buffer.len() as u64) as usize;
                old_file.read_exact(&mut buffer[..to_read]).await?;
                new_file.write_all(&buffer[..to_read]).await?;
                current_new_offset = current_new_offset.wrapping_add(to_read as u64);
                remaining -= to_read as u64;
            }

            new_index_map.insert(
                id,
                ChunkMeta {
                    offset: entry_start,
                    length: meta.length,
                    deleted: false,
                    created_at: meta.created_at,
                },
            );
        }

        new_file.flush().await?;

        fs::rename(&tmp_path, &path).await?;

        {
            let mut idx = index_lock.write().await;
            *idx = new_index_map;
        }

        Ok(())
    }

    pub async fn gc_all(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for p in 0..self.partition_count {
            self.gc_partition(p).await?;
        }
        Ok(())
    }
}

/// Public metadata for a chunk
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    pub chunk_id: u64,
    pub offset: u64,
    pub size: u64,
    pub created_at: i64,
    pub partition: u32,
}

/// Chunk data with metadata
pub struct ChunkWithMetadata {
    pub metadata: ChunkMetadata,
    pub stream: Box<dyn Stream<Item = Vec<u8>> + Unpin + Send>,
}
