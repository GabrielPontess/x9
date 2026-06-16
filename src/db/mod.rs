use sqlx::{sqlite::SqliteConnectOptions, SqlitePool, Row};
use std::fs;
use std::path::Path;
use std::str::FromStr;

pub struct DbManager {
    pool: SqlitePool,
}

impl DbManager {
    pub async fn new(db_url: &str) -> Result<Self, sqlx::Error> {
        if let Some(file_path_str) = db_url.strip_prefix("sqlite://") {
            let path = Path::new(file_path_str);
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).map_err(sqlx::Error::Io)?;
                }
            }
        }

        let options = SqliteConnectOptions::from_str(db_url)?.create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS transfer_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_path TEXT NOT NULL UNIQUE,
                dest_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                bytes_transferred INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'Pending',
                error_message TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );"
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    pub async fn upsert_file(&self, source: &str, dest: &str, size: u64, status: &str, error: Option<&str>) -> Result<i64, sqlx::Error> {
        sqlx::query(
            "INSERT INTO transfer_queue (source_path, dest_path, file_size, bytes_transferred, status, error_message)
             VALUES (?, ?, ?, 0, ?, ?)
             ON CONFLICT(source_path) DO UPDATE SET
                 dest_path = excluded.dest_path,
                 file_size = excluded.file_size,
                 bytes_transferred = excluded.bytes_transferred,
                 status = excluded.status,
                 error_message = excluded.error_message,
                 updated_at = CURRENT_TIMESTAMP"
        )
        .bind(source)
        .bind(dest)
        .bind(size as i64)
        .bind(status)
        .bind(error)
        .execute(&self.pool)
        .await?;

        let row = sqlx::query("SELECT id FROM transfer_queue WHERE source_path = ?")
            .bind(source)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.get(0))
    }

    pub async fn update_status(&self, id: i64, status: &str, error: Option<&str>) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE transfer_queue SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        )
        .bind(status)
        .bind(error)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_progress(&self, id: i64, bytes: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE transfer_queue SET bytes_transferred = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?"
        )
        .bind(bytes)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn record_file_status(&self, source: &str, dest: &str, size: u64, status: &str, error: Option<&str>) -> Result<(), sqlx::Error> {
        let _ = self.upsert_file(source, dest, size, status, error).await?;
        Ok(())
    }

    pub async fn clear_processing_status(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE transfer_queue
             SET status = 'Failed',
                 error_message = 'Interrompido antes de concluir',
                 updated_at = CURRENT_TIMESTAMP
             WHERE status = 'Processing'"
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
