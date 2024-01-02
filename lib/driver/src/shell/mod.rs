use self::{highlighter::SqlHighlighter, prompt::SqlPrompt};
use crate::DriverRef;
use anyhow::Result;
use nu_ansi_term::{Color, Style};
use owo_colors::OwoColorize;
use prettytable::{row, Table};
use reedline::{DefaultHinter, DefaultPrompt, FileBackedHistory, Reedline, Signal};
use typed_builder::TypedBuilder;

mod highlighter;
mod prompt;

#[derive(TypedBuilder)]
pub struct Shell {
    driver: DriverRef,
    prompt: SqlPrompt,
    line_editor: Reedline,
    bail_on_error: bool,
}

impl Shell {
    pub fn new(driver: DriverRef) -> Self {
        let prompt = SqlPrompt::default();
        let highlighter = SqlHighlighter::new();
        let history = FileBackedHistory::with_file(100, "history.txt".into())
            .expect("Unable to create history file");
        let hinter =
            DefaultHinter::default().with_style(Style::new().italic().fg(Color::LightGray));

        let line_editor = Reedline::create()
            .with_highlighter(Box::new(highlighter))
            .with_history(Box::new(history))
            .with_hinter(Box::new(hinter));

        Shell::builder()
            .driver(driver)
            .prompt(prompt)
            .line_editor(line_editor)
            .bail_on_error(false)
            .build()
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            let input = self.line_editor.read_line(&self.prompt)?;
            match input {
                Signal::Success(buffer) => {
                    self.prompt.increment_line_count();

                    if buffer.trim().is_empty() {
                        continue;
                    }

                    if let Err(e) = self.process_command(buffer.trim()).await {
                        if self.bail_on_error {
                            return Err(e);
                        }
                    }
                }
                Signal::CtrlC | Signal::CtrlD => {
                    println!("Goodbye!");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn process_command(&mut self, command: &str) -> Result<()> {
        if command.starts_with('.') {
            self.handle_dot_command(command)?;
        } else {
            self.driver.process_sql_command(&command.to_string()).await;
        }

        Ok(())
    }

    fn handle_dot_command(&mut self, command: &str) -> Result<()> {
        match command.split_whitespace().collect::<Vec<&str>>().as_slice() {
            [".bail"] => {
                println!(
                    "{}",
                    format!(
                        "Error stop mode is {}",
                        if self.bail_on_error {
                            "on".green().to_string()
                        } else {
                            "off".red().to_string()
                        }
                    )
                    .purple()
                );
                Ok(())
            }
            [".bail", "on"] => {
                self.bail_on_error = true;
                Ok(())
            }
            [".bail", "off"] => {
                self.bail_on_error = false;
                Ok(())
            }
            [".binary", "on"] => {
                todo!("Add binary output");
                // self.driver.set_binary_output(true);
                // Ok(())
            }
            [".binary", "off"] => {
                todo!("Add binary output");
                // self.driver.set_binary_output(false);
                // Ok(())
            }
            [".exit"] => {
                println!("Goodbye!");
                std::process::exit(0);
            }
            [".exit", code] => {
                println!("Goodbye!");
                std::process::exit(code.parse::<i32>().unwrap_or(0));
            }
            [".help"] => {
                self.show_help();
                Ok(())
            }
            [".quit"] => {
                println!("Goodbye!");
                std::process::exit(0);
            }
            [".tables"] => {
                todo!("Add table listing");
                // self.driver.show_tables();
                // Ok(())
            }
            [".tables", table] => {
                todo!("Add table listing");
                // self.driver.show_tables_like(table);
                // Ok(())
            }
            [".vfslist"] => {
                todo!("Add VFS listing");
                // self.driver.show_vfs_list();
                // Ok(())
            }
            _ => {
                println!(
                    "{}{}{}{}{}",
                    format!("Unrecognized dot command").purple(),
                    format!(":").black(),
                    format!(" `").red(),
                    format!("{}", command).yellow(),
                    format!("`").red(),
                );
                Ok(())
            }
        }
    }

    fn show_help(&self) {
        // Table for general help
        let mut table = Table::new();

        // TODO: Add more help information (e.g., how to use the shell, etc.)
        // TODO: add semantic highlighting using owo_colors (e.g., for commands, etc.)

        table.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

        table.set_titles(row![
            "General Help",
            "Type \".help <command>\" for help on <command>",
        ]);

        table.add_row(row![
            "Press Ctrl+D or type .exit to exit",
            "Exit this program",
        ]);
        table.add_row(row![
            "Press Ctrl+C to cancel the current command",
            "Cancel the current command"
        ]);
        table.add_row(row!["Press Ctrl+L to clear the screen", "Clear the screen"]);
        table.add_row(row![
            "Press Ctrl+R to search through command history",
            "Search through command history"
        ]);
        table.add_row(row![
            "Press Up/Down arrows or Ctrl+n/p to browse history",
            "Browse history"
        ]);

        table.printstd();

        let mut table = Table::new();

        println!();
        println!("Available dot commands: (type \".help <command>\" for details)");

        // Available commands
        table.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

        table.set_titles(row!["Command", "Description"]);

        table.add_row(row![".bail", "Toggle error stop mode"]);
        table.add_row(row![".binary", "Toggle binary output mode"]);
        table.add_row(row![
            ".exit [CODE]",
            "Exit this program with return-code [CODE]"
        ]);
        table.add_row(row![".help", "Show this help information"]);
        table.add_row(row![".quit", "Exit this program (with return-code 0)"]);
        table.add_row(row![
            ".tables [TABLE]",
            "List names of tables matching LIKE pattern [TABLE]"
        ]);
        table.add_row(row![".vfslist", "List all available VFSes"]);

        table.printstd();
    }
}
