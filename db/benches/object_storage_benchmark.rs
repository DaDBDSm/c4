use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use db::storage::simple::ObjectStorageSimple;
use db::storage::{CreateBucketDTO, DeleteObjectDTO, GetObjectDTO, ObjectStorage, PutObjectDTO};
use tempfile::tempdir;
use tokio::runtime::{Builder, Runtime};
use tokio_stream::{self as stream, StreamExt};

async fn create_storage() -> (ObjectStorageSimple, tempfile::TempDir) {
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let base_dir = temp_dir.path().to_path_buf();

    let bytes_storage = db::storage::simple::chunk_file_storage::PartitionedBytesStorage::new(
        base_dir.join("data"),
        4,
    );

    let buckets_metadata_storage =
        db::storage::simple::buckets_metadata_storage::BucketsMetadataStorage::new(
            base_dir.join("metadata").to_string_lossy().to_string(),
        )
        .await
        .expect("Failed to create buckets metadata storage");

    let storage = ObjectStorageSimple {
        base_dir,
        bytes_storage,
        buckets_metadata_storage,
    };

    (storage, temp_dir)
}

fn create_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

fn create_stream(data: Vec<u8>) -> impl stream::Stream<Item = Vec<u8>> + Unpin + Send + 'static {
    stream::iter(vec![data])
}

fn create_single_threaded_runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create single-threaded runtime")
}

fn create_multi_threaded_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("Failed to create multi-threaded runtime")
}

fn bench_put_operation(
    c: &mut Criterion,
    group_name: &str,
    sizes: &[usize],
    runtime: &Runtime,
    runtime_type: &str,
) {
    let mut group = c.benchmark_group(format!("{}_{}", group_name, runtime_type));

    for size in sizes.iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("put_object", size), size, |b, &size| {
            b.iter(|| {
                runtime.block_on(async {
                    let (storage, _temp_dir) = create_storage().await;

                    storage
                        .create_bucket(CreateBucketDTO {
                            bucket_name: "test-bucket".to_string(),
                        })
                        .await
                        .expect("Failed to create bucket");

                    let test_data = create_test_data(size);
                    let stream = create_stream(test_data.clone());

                    black_box(
                        storage
                            .put_object(PutObjectDTO {
                                bucket_name: "test-bucket".to_string(),
                                key: "test-object".to_string(),
                                stream: Box::new(stream),
                            })
                            .await
                            .expect("Failed to put object"),
                    );
                });
            });
        });
    }

    group.finish();
}

fn bench_get_operation(
    c: &mut Criterion,
    group_name: &str,
    sizes: &[usize],
    runtime: &Runtime,
    runtime_type: &str,
) {
    let mut group = c.benchmark_group(format!("{}_{}", group_name, runtime_type));

    for size in sizes.iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::new("get_object", size), size, |b, &size| {
            b.iter(|| {
                runtime.block_on(async {
                    let (storage, _temp_dir) = create_storage().await;

                    storage
                        .create_bucket(CreateBucketDTO {
                            bucket_name: "test-bucket".to_string(),
                        })
                        .await
                        .expect("Failed to create bucket");

                    let test_data = create_test_data(size);
                    let put_stream = create_stream(test_data.clone());

                    storage
                        .put_object(PutObjectDTO {
                            bucket_name: "test-bucket".to_string(),
                            key: "test-object".to_string(),
                            stream: Box::new(put_stream),
                        })
                        .await
                        .expect("Failed to put object");

                    let mut stream = storage
                        .get_object(GetObjectDTO {
                            bucket_name: "test-bucket".to_string(),
                            key: "test-object".to_string(),
                        })
                        .await
                        .expect("Failed to get object");

                    let mut total_bytes = 0;
                    while let Some(chunk) = stream.next().await {
                        total_bytes += chunk.len();
                        black_box(chunk);
                    }

                    assert_eq!(total_bytes, size);
                });
            });
        });
    }

    group.finish();
}

fn bench_put_get_delete_operation(
    c: &mut Criterion,
    group_name: &str,
    sizes: &[usize],
    runtime: &Runtime,
    runtime_type: &str,
) {
    let mut group = c.benchmark_group(format!("{}_{}", group_name, runtime_type));

    for size in sizes.iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("put_get_delete", size),
            size,
            |b, &size| {
                b.iter(|| {
                    runtime.block_on(async {
                        let (storage, _temp_dir) = create_storage().await;

                        storage
                            .create_bucket(CreateBucketDTO {
                                bucket_name: "test-bucket".to_string(),
                            })
                            .await
                            .expect("Failed to create bucket");

                        let test_data = create_test_data(size);
                        let stream = create_stream(test_data.clone());

                        let metadata = storage
                            .put_object(PutObjectDTO {
                                bucket_name: "test-bucket".to_string(),
                                key: "test-object".to_string(),
                                stream: Box::new(stream),
                            })
                            .await
                            .expect("Failed to put object");

                        let mut get_stream = storage
                            .get_object(GetObjectDTO {
                                bucket_name: "test-bucket".to_string(),
                                key: "test-object".to_string(),
                            })
                            .await
                            .expect("Failed to get object");

                        let mut total_bytes = 0;
                        while let Some(chunk) = get_stream.next().await {
                            total_bytes += chunk.len();
                            black_box(chunk);
                        }

                        assert_eq!(total_bytes, size);

                        storage
                            .delete_object(DeleteObjectDTO {
                                bucket_name: "test-bucket".to_string(),
                                key: "test-object".to_string(),
                            })
                            .await
                            .expect("Failed to delete object");

                        black_box(metadata);
                    });
                });
            },
        );
    }

    group.finish();
}

fn bench_small_objects(c: &mut Criterion) {
    let single_threaded_rt = create_single_threaded_runtime();
    let multi_threaded_rt = create_multi_threaded_runtime();

    let sizes = [1024];

    bench_put_operation(
        c,
        "small_objects",
        &sizes,
        &single_threaded_rt,
        "single_threaded",
    );
    bench_get_operation(
        c,
        "small_objects",
        &sizes,
        &single_threaded_rt,
        "single_threaded",
    );
    bench_put_get_delete_operation(
        c,
        "small_objects",
        &sizes,
        &single_threaded_rt,
        "single_threaded",
    );

    bench_put_operation(
        c,
        "small_objects",
        &sizes,
        &multi_threaded_rt,
        "multi_threaded",
    );
    bench_get_operation(
        c,
        "small_objects",
        &sizes,
        &multi_threaded_rt,
        "multi_threaded",
    );
    bench_put_get_delete_operation(
        c,
        "small_objects",
        &sizes,
        &multi_threaded_rt,
        "multi_threaded",
    );
}

fn bench_average_objects(c: &mut Criterion) {
    let single_threaded_rt = create_single_threaded_runtime();
    let multi_threaded_rt = create_multi_threaded_runtime();

    let sizes = [1048576];
    bench_put_operation(
        c,
        "average_objects",
        &sizes,
        &single_threaded_rt,
        "single_threaded",
    );
    bench_get_operation(
        c,
        "average_objects",
        &sizes,
        &single_threaded_rt,
        "single_threaded",
    );
    bench_put_get_delete_operation(
        c,
        "average_objects",
        &sizes,
        &single_threaded_rt,
        "single_threaded",
    );

    bench_put_operation(
        c,
        "average_objects",
        &sizes,
        &multi_threaded_rt,
        "multi_threaded",
    );
    bench_get_operation(
        c,
        "average_objects",
        &sizes,
        &multi_threaded_rt,
        "multi_threaded",
    );
    bench_put_get_delete_operation(
        c,
        "average_objects",
        &sizes,
        &multi_threaded_rt,
        "multi_threaded",
    );
}

criterion_group!(benches, bench_small_objects, bench_average_objects,);
criterion_main!(benches);
