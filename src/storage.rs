use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode,
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path as ObjectPath,
};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object key must be a non-empty relative path without `..`")]
    InvalidKey,
    #[error("object already exists")]
    AlreadyExists,
    #[error("object was not found")]
    NotFound,
    #[error("local storage I/O failed")]
    Io(#[source] std::io::Error),
    #[error("remote object storage request failed: {0}")]
    Remote(String),
    #[error("invalid storage configuration: {0}")]
    Configuration(String),
}

#[async_trait]
pub trait BlobStorage: Send + Sync {
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError>;
    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError>;
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
}

pub type SharedBlobStorage = Arc<dyn BlobStorage>;

pub fn dataset_source_key(dataset_id: uuid::Uuid) -> String {
    format!("datasets/{dataset_id}/source.csv")
}

pub fn dataset_partition_key(dataset_id: uuid::Uuid, partition_number: i32) -> String {
    format!("datasets/{dataset_id}/partitions/part-{partition_number:05}.csv")
}

pub fn task_result_key(job_id: uuid::Uuid, task_id: uuid::Uuid) -> String {
    format!("jobs/{job_id}/tasks/{task_id}/result.json")
}

pub fn job_result_key(job_id: uuid::Uuid) -> String {
    format!("jobs/{job_id}/result.json")
}

#[derive(Debug, Clone)]
pub struct LocalBlobStorage {
    root: PathBuf,
}

impl LocalBlobStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn object_path(&self, key: &str) -> Result<PathBuf, StorageError> {
        let key_path = Path::new(key);
        if key.is_empty()
            || key_path.is_absolute()
            || key_path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(StorageError::InvalidKey);
        }

        Ok(self.root.join(key_path))
    }
}

#[async_trait]
impl BlobStorage for LocalBlobStorage {
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        let path = self.object_path(key)?;
        let parent = path.parent().ok_or(StorageError::InvalidKey)?;

        tokio::fs::create_dir_all(parent)
            .await
            .map_err(map_io_error)?;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .map_err(map_io_error)?;
        file.write_all(bytes).await.map_err(map_io_error)?;
        file.flush().await.map_err(map_io_error)?;

        tracing::debug!(
            object_key = key,
            byte_count = bytes.len(),
            "Stored local blob"
        );
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let path = self.object_path(key)?;
        let bytes = tokio::fs::read(path).await.map_err(map_io_error)?;

        tracing::debug!(
            object_key = key,
            byte_count = bytes.len(),
            "Read local blob"
        );
        Ok(bytes)
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let path = self.object_path(key)?;
        tokio::fs::remove_file(path).await.map_err(map_io_error)?;

        tracing::debug!(object_key = key, "Deleted local blob");
        Ok(())
    }
}

fn map_io_error(error: std::io::Error) -> StorageError {
    match error.kind() {
        std::io::ErrorKind::AlreadyExists => StorageError::AlreadyExists,
        std::io::ErrorKind::NotFound => StorageError::NotFound,
        _ => StorageError::Io(error),
    }
}

pub struct MinioBlobStorage {
    store: Arc<dyn ObjectStore>,
}

impl MinioBlobStorage {
    pub fn from_env() -> Result<Self, StorageError> {
        let endpoint = required_env("RUSTFLOW_S3_ENDPOINT")?;
        let bucket = required_env("RUSTFLOW_S3_BUCKET")?;
        let access_key = required_env("RUSTFLOW_S3_ACCESS_KEY")?;
        let secret_key = required_env("RUSTFLOW_S3_SECRET_KEY")?;
        let region =
            std::env::var("RUSTFLOW_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());

        let store = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .map_err(|error| StorageError::Configuration(error.to_string()))?;

        Ok(Self {
            store: Arc::new(store),
        })
    }

    fn object_path(key: &str) -> Result<ObjectPath, StorageError> {
        validate_key(key)?;
        Ok(ObjectPath::from(key))
    }
}

#[async_trait]
impl BlobStorage for MinioBlobStorage {
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        let path = Self::object_path(key)?;
        self.store
            .put_opts(&path, bytes.to_vec().into(), PutMode::Create.into())
            .await
            .map_err(map_remote_error)?;
        tracing::debug!(
            object_key = key,
            byte_count = bytes.len(),
            "Stored MinIO blob"
        );
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let path = Self::object_path(key)?;
        let bytes = self
            .store
            .get(&path)
            .await
            .map_err(map_remote_error)?
            .bytes()
            .await
            .map_err(map_remote_error)?;
        tracing::debug!(
            object_key = key,
            byte_count = bytes.len(),
            "Read MinIO blob"
        );
        Ok(bytes.to_vec())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let path = Self::object_path(key)?;
        self.store.delete(&path).await.map_err(map_remote_error)?;
        tracing::debug!(object_key = key, "Deleted MinIO blob");
        Ok(())
    }
}

pub fn storage_from_env() -> Result<SharedBlobStorage, StorageError> {
    match std::env::var("RUSTFLOW_STORAGE_BACKEND")
        .unwrap_or_else(|_| "local".to_string())
        .as_str()
    {
        "local" => Ok(Arc::new(LocalBlobStorage::new(
            std::env::var("RUSTFLOW_LOCAL_STORAGE_ROOT")
                .unwrap_or_else(|_| "data/objects".to_string()),
        ))),
        "minio" => Ok(Arc::new(MinioBlobStorage::from_env()?)),
        backend => Err(StorageError::Configuration(format!(
            "unsupported RUSTFLOW_STORAGE_BACKEND: {backend}"
        ))),
    }
}

pub async fn put_or_verify(
    storage: &dyn BlobStorage,
    key: &str,
    bytes: &[u8],
) -> Result<(), StorageError> {
    match storage.put(key, bytes).await {
        Ok(()) => Ok(()),
        Err(StorageError::AlreadyExists) => {
            let existing = storage.get(key).await?;
            if existing == bytes {
                Ok(())
            } else {
                Err(StorageError::AlreadyExists)
            }
        }
        Err(error) => Err(error),
    }
}

pub struct MaterializedBlob {
    path: PathBuf,
}

impl MaterializedBlob {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn cleanup(self) {
        if let Err(error) = tokio::fs::remove_file(&self.path).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %self.path.display(), %error, "Could not remove materialized blob");
        }
    }
}

pub async fn materialize_blob(
    storage: &dyn BlobStorage,
    key: Option<&str>,
    legacy_path: &str,
) -> Result<MaterializedBlob, StorageError> {
    let bytes = read_blob_or_legacy(storage, key, legacy_path).await?;
    let directory = std::env::temp_dir().join("rustflow-materialized");
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(map_io_error)?;
    let path = directory.join(format!("{}.csv", uuid::Uuid::new_v4()));
    tokio::fs::write(&path, bytes).await.map_err(map_io_error)?;
    Ok(MaterializedBlob { path })
}

pub async fn read_blob_or_legacy(
    storage: &dyn BlobStorage,
    key: Option<&str>,
    legacy_path: &str,
) -> Result<Vec<u8>, StorageError> {
    match key {
        Some(key) => storage.get(key).await,
        None => tokio::fs::read(legacy_path).await.map_err(map_io_error),
    }
}

fn validate_key(key: &str) -> Result<(), StorageError> {
    let key_path = Path::new(key);
    if key.is_empty()
        || key_path.is_absolute()
        || key_path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(StorageError::InvalidKey);
    }
    Ok(())
}

fn required_env(name: &str) -> Result<String, StorageError> {
    std::env::var(name).map_err(|_| StorageError::Configuration(format!("{name} is required")))
}

fn map_remote_error(error: object_store::Error) -> StorageError {
    match error {
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. } => {
            StorageError::AlreadyExists
        }
        object_store::Error::NotFound { .. } => StorageError::NotFound,
        error => StorageError::Remote(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{BlobStorage, LocalBlobStorage, StorageError};

    fn test_root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("rustflow-storage-test-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn local_storage_saves_reads_and_deletes_an_object() {
        let root = test_root();
        let storage = LocalBlobStorage::new(&root);
        let key = "datasets/example/source.csv";

        storage.put(key, b"id,name\n1,Alice\n").await.unwrap();
        assert_eq!(storage.get(key).await.unwrap(), b"id,name\n1,Alice\n");

        storage.delete(key).await.unwrap();
        assert!(matches!(
            storage.get(key).await,
            Err(StorageError::NotFound)
        ));

        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn local_storage_never_overwrites_an_existing_object() {
        let root = test_root();
        let storage = LocalBlobStorage::new(&root);
        let key = "datasets/example/source.csv";

        storage.put(key, b"first").await.unwrap();
        assert!(matches!(
            storage.put(key, b"second").await,
            Err(StorageError::AlreadyExists)
        ));
        assert_eq!(storage.get(key).await.unwrap(), b"first");

        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn local_storage_rejects_unsafe_object_keys() {
        let root = test_root();
        let storage = LocalBlobStorage::new(&root);

        for key in ["", "../secret.csv", "/tmp/secret.csv", "."] {
            assert!(matches!(
                storage.put(key, b"test").await,
                Err(StorageError::InvalidKey)
            ));
        }
    }
}
