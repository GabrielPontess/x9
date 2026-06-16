mod db;
mod tui;
mod engines;

use db::DbManager;
use engines::copy_engine::CopyEngine;
use tui::TuiTerminal;

use std::{error::Error, sync::Arc, time::Duration};
use tokio::sync::mpsc;

pub enum AppEvent {
    Log(String),
    CurrentFile(Option<String>),
    StatsUpdate(TransferStat),
    Quit,
}

pub enum TransferStat {
    Completed,
    Skipped,
    Failed,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let smb_mount = r"\\172.16.8.31\Backups\CBPM\SistemaDiamond\".to_string();
    let dest_hd = r"D:\Teste_X9\".to_string();

    let db_manager = Arc::new(DbManager::new("sqlite://db/queue.db").await?);
    let (tx, mut rx) = mpsc::channel::<AppEvent>(1000);

    let tx_keys = tx.clone();
    tokio::spawn(async move {
        loop {
            if crossterm::event::poll(Duration::from_millis(50)).unwrap() {
                if let crossterm::event::Event::Key(key) = crossterm::event::read().unwrap() {
                    if key.code == crossterm::event::KeyCode::Char('q') || key.code == crossterm::event::KeyCode::Esc {
                        let _ = tx_keys.send(AppEvent::Quit).await;
                        break;
                    }
                }
            }
        }
    });

    let tx_ctrl_c = tx.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = tx_ctrl_c.send(AppEvent::Quit).await;
        }
    });

    let mut terminal = TuiTerminal::new()?;
    let mut app_logs = vec![String::from("Aplicacao x9 iniciada. Aperte Q, ESC ou Ctrl+C para sair.")];
    let mut status_line = String::from("Preparando transferencia em streaming...");
    let mut current_file: Option<String> = None;
    let mut completed_count: u64 = 0;
    let mut skipped_count: u64 = 0;
    let mut failed_count: u64 = 0;

    let db_engine = Arc::clone(&db_manager);
    let tx_engine = tx.clone();

    tokio::spawn(async move {
        let engine = CopyEngine::new(db_engine.clone(), tx_engine.clone());

        if let Err(e) = engine.run_transfer_stream(smb_mount, dest_hd, 3).await {
            let _ = tx_engine.send(AppEvent::Log(format!("Erro na transferencia: {}", e))).await;
        }
    });

    let mut render_interval = tokio::time::interval(Duration::from_millis(33));
    render_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = render_interval.tick() => {
                terminal.draw(
                    &status_line,
                    current_file.as_deref(),
                    completed_count,
                    skipped_count,
                    failed_count,
                    &app_logs,
                )?;
            }

            _ = tokio::signal::ctrl_c() => {
                app_logs.push("Ctrl+C detectado! Encerrando imediatamente...".to_string());
                status_line = String::from("Encerrando por Ctrl+C...");
                terminal.draw(
                    &status_line,
                    current_file.as_deref(),
                    completed_count,
                    skipped_count,
                    failed_count,
                    &app_logs,
                )?;
                tokio::time::sleep(Duration::from_millis(300)).await;
                break;
            }

            Some(event) = rx.recv() => {
                match event {
                    AppEvent::Quit => {
                        app_logs.push("Parada solicitada. Restaurando terminal...".to_string());
                        status_line = String::from("Parada solicitada pelo usuario.");
                        terminal.draw(
                            &status_line,
                            current_file.as_deref(),
                            completed_count,
                            skipped_count,
                            failed_count,
                            &app_logs,
                        )?;
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        break;
                    }
                    AppEvent::Log(msg) => {
                        if app_logs.len() > 50 {
                            app_logs.remove(0);
                        }

                        if msg.contains("Copiando:") {
                            status_line = String::from("Transferindo arquivo...");
                        } else if msg.contains("Todas as transferencias") {
                            status_line = String::from("Transferencia concluida.");
                            current_file = None;
                        }

                        app_logs.push(msg);
                    }
                    AppEvent::CurrentFile(file) => {
                        current_file = file;
                        if current_file.is_none() && status_line == "Transferindo arquivo..." {
                            status_line = String::from("Aguardando proximo arquivo...");
                        }
                    }
                    AppEvent::StatsUpdate(stat) => {
                        match stat {
                            TransferStat::Completed => completed_count += 1,
                            TransferStat::Skipped => skipped_count += 1,
                            TransferStat::Failed => failed_count += 1,
                        }
                    }
                }
            }
            else => break,
        }
    }

    Ok(())
}
