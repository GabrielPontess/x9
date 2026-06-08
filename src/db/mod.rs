use sqlx::{SqlitePool, Row};
use std::path::Path;

pub struct DbManager {
    pool: SqlitePool,
}

impl DbManager {
    pub async fn new(db_url: &str) -> Result<Self, sqlx::Error> {
        let pool = SqlitePool::connect(db_url).await?;
        
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

    pub async fn queue_file(&self, source: &str, dest: &str, size: u64, status: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO transfer_queue (source_path, dest_path, file_size, status) 
             VALUES (?, ?, ?, ?) 
             ON CONFLICT(source_path) DO NOTHING"
        )
        .bind(source)
        .bind(dest)
        .bind(size as i64)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}