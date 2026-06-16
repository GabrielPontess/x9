use std::io::{self, Stdout};

use crossterm::{
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{prelude::*, widgets::*};

pub struct TuiTerminal {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TuiTerminal {
    pub fn new() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self { terminal })
    }

    pub fn draw(&mut self, status_line: &str, current_file: Option<&str>, logs: &[String]) -> io::Result<()> {
        self.terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Min(5),
                    Constraint::Length(8),
                ])
                .split(f.area());

            let status = Paragraph::new(status_line)
                .block(Block::default().title(" Status x9 ").borders(Borders::ALL));
            f.render_widget(status, chunks[0]);

            let items_progress = [ListItem::new(current_file.unwrap_or("Nenhum arquivo sendo transferido no momento..."))];
            let list = List::new(items_progress).block(Block::default().title(" Transferencias Ativas ").borders(Borders::ALL));
            f.render_widget(list, chunks[1]);

            let log_items: Vec<ListItem> = logs.iter().map(|log| ListItem::new(log.as_str())).collect();
            let log_list = List::new(log_items).block(Block::default().title(" Logs em Tempo Real ").borders(Borders::ALL));
            f.render_widget(log_list, chunks[2]);
        })?;
        Ok(())
    }
}

impl Drop for TuiTerminal {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
    }
}
