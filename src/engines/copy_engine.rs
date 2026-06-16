use std::path::Path;
use std::sync::Arc;

use filetime::{set_file_times, FileTime};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use walkdir::WalkDir;

use crate::db::DbManager;
use crate::{AppEvent, TransferStat};

pub struct CopyEngine {
    db: Arc<DbManager>,
    tx_tui: mpsc::Sender<AppEvent>,
}

struct DiscoveredFile {
    source_path: String,
    dest_path: String,
    file_size: u64,
}

impl CopyEngine {
    pub fn new(db: Arc<DbManager>, tx_tui: mpsc::Sender<AppEvent>) -> Self {
        Self { db, tx_tui }
    }

    pub async fn run_transfer_stream(&self, smb_mount: String, dest_hd: String, max_concurrent_tasks: usize) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _ = self.db.clear_processing_status().await;
        let _ = self.tx_tui.send(AppEvent::Log(format!("Mapeando origem UNC: {}", smb_mount))).await;
        let _ = self.tx_tui.send(AppEvent::Log(format!("Iniciando transferencia com {} workers.", max_concurrent_tasks))).await;

        let (discovery_tx, mut discovery_rx) = tokio::sync::mpsc::unbounded_channel::<DiscoveredFile>();
        let scan_root = smb_mount.clone();
        let dest_root = dest_hd.clone();
        let log_tx = self.tx_tui.clone();

        std::thread::spawn(move || {
            let _ = log_tx.blocking_send(AppEvent::Log("Descobrindo arquivos e liberando copia em streaming...".to_string()));

            let walker = WalkDir::new(&scan_root).max_open(10).into_iter();

            for entry_result in walker {
                let entry = match entry_result {
                    Ok(entry) => entry,
                    Err(err) => {
                        let _ = log_tx.blocking_send(AppEvent::Log(format!("Erro ao varrer diretorio: {}", err)));
                        continue;
                    }
                };

                let path = entry.path();
                if !path.is_file() {
                    continue;
                }

                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        let _ = log_tx.blocking_send(AppEvent::Log(format!("Nao foi possivel ler metadados de {}: {}", path.display(), err)));
                        continue;
                    }
                };

                let relative_path = match path.strip_prefix(&scan_root) {
                    Ok(relative_path) => relative_path,
                    Err(_) => continue,
                };

                let target_path = Path::new(&dest_root).join(relative_path);
                let file = DiscoveredFile {
                    source_path: path.to_string_lossy().to_string(),
                    dest_path: target_path.to_string_lossy().to_string(),
                    file_size: metadata.len(),
                };

                if discovery_tx.send(file).is_err() {
                    break;
                }
            }
        });

        let mut workers = Vec::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_tasks));

        while let Some(file_job) = discovery_rx.recv().await {
            if Path::new(&file_job.dest_path).exists() {
                let _ = self.db.record_file_status(&file_job.source_path, &file_job.dest_path, file_job.file_size, "Skipped", None).await;
                let _ = self.tx_tui.send(AppEvent::Log(format!("Ja existe no destino: {}", file_job.source_path))).await;
                let _ = self.tx_tui.send(AppEvent::StatsUpdate(TransferStat::Skipped)).await;
                continue;
            }

            let permit = semaphore.clone().acquire_owned().await?;
            let db = Arc::clone(&self.db);
            let tx = self.tx_tui.clone();

            let handle = tokio::spawn(async move {
                let _permit = permit;

                let job_id = match db.upsert_file(&file_job.source_path, &file_job.dest_path, file_job.file_size, "Processing", None).await {
                    Ok(job_id) => job_id,
                    Err(err) => {
                        let _ = tx.send(AppEvent::Log(format!("Erro ao registrar {} no banco: {}", file_job.source_path, err))).await;
                        return;
                    }
                };

                let _ = tx.send(AppEvent::CurrentFile(Some(file_job.source_path.clone()))).await;
                let _ = tx.send(AppEvent::Log(format!("Copiando: {}", file_job.source_path))).await;

                match Self::copy_single_file(&file_job.source_path, &file_job.dest_path, job_id, &db).await {
                    Ok(_) => {
                        let _ = db.update_status(job_id, "Completed", None).await;
                        let _ = tx.send(AppEvent::Log(format!("Sucesso: {}", file_job.source_path))).await;
                        let _ = tx.send(AppEvent::StatsUpdate(TransferStat::Completed)).await;
                    }
                    Err(err) => {
                        let err_msg = err.to_string();
                        let _ = db.update_status(job_id, "Failed", Some(&err_msg)).await;
                        let _ = tx.send(AppEvent::Log(format!("Erro em {}: {}", file_job.source_path, err_msg))).await;
                        let _ = tx.send(AppEvent::StatsUpdate(TransferStat::Failed)).await;
                    }
                }

                let _ = tx.send(AppEvent::CurrentFile(None)).await;
            });

            workers.push(handle);
        }

        for worker in workers {
            let _ = worker.await;
        }

        let _ = self.tx_tui.send(AppEvent::CurrentFile(None)).await;
        let _ = self.tx_tui.send(AppEvent::Log("Todas as transferencias foram processadas!".to_string())).await;
        Ok(())
    }

    async fn copy_single_file(
        source: &str,
        dest: &str,
        job_id: i64,
        db: &DbManager,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let src_path = Path::new(source);
        let dest_path = Path::new(dest);

        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut src_file = File::open(src_path).await?;
        let mut dest_file = File::create(dest_path).await?;

        let mut buffer = vec![0; 64 * 1024];
        let mut total_copied = 0;

        loop {
            let bytes_read = src_file.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }

            dest_file.write_all(&buffer[..bytes_read]).await?;
            total_copied += bytes_read as i64;
            let _ = db.update_progress(job_id, total_copied).await;
        }

        dest_file.flush().await?;

        let src_metadata = std::fs::metadata(src_path)?;
        let mtime = FileTime::from_last_modification_time(&src_metadata);
        let atime = FileTime::from_last_access_time(&src_metadata);
        set_file_times(dest_path, atime, mtime)?;

        Ok(())
    }
}
