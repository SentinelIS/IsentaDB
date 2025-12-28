// src/lib.rs

// The modules are declared public so they can be used by binary crates (CLI, server).
pub mod storage;
pub mod parser;
pub mod engine;
pub mod database;
pub mod wal;

use parser::{Command, Parser};
use engine::QueryEngine;

/// Executes a single line of input against the query engine.
///
/// This function encapsulates the command processing logic, taking a line of text,
/// parsing it, executing the corresponding command, and returning the result
/// as a formatted string. This allows both the CLI and the server to share the
/// same command execution logic without duplicating code or printing directly to stdout.
///
/// # Arguments
/// * `input` - A string slice representing the command to be executed.
/// * `query_engine` - A mutable reference to the `QueryEngine` instance.
/// * `parser` - A reference to the `Parser` instance.
///
/// # Returns
/// A `String` containing the formatted result of the command execution, ready to be
/// displayed to a user or sent over a network connection.
pub fn execute_line(input: &str, query_engine: &mut QueryEngine, parser: &Parser) -> String {
    // Skip empty input
    if input.is_empty() {
        return String::new(); // Return an empty string for empty input
    }

    // Handle special commands that don't require SQL parsing.
    // This logic is kept separate from the SQL command parsing.
    match input.to_lowercase().as_str() {
        "help" => {
            return print_help();
        }
        // Note: "exit" and "quit" are not handled here because they are process-specific.
        // The caller (CLI or server) is responsible for managing its own lifecycle.
        _ => {}
    }

    // Parse and execute the SQL command using the provided parser.
    let command = parser.parse(input);
    match command {
        Command::CreateTable { name, columns } => {
            match query_engine.execute_create_table(name.clone(), columns) {
                Ok(_) => format!("Table '{}' created successfully", name),
                Err(e) => format!("Error: {}", e),
            }
        }
        Command::Insert { table, values } => {
            match query_engine.execute_insert(table.clone(), values) {
                Ok(_) => format!("Inserted 1 row into '{}'", table),
                Err(e) => format!("Error: {}", e),
            }
        }
        Command::Select { table, columns, where_clause } => {
            match query_engine.execute_select(table.clone(), columns, where_clause) {
                Ok((cols, rows)) => {
                    if rows.is_empty() {
                        format!("No rows found in '{}'", table)
                    } else {
                        // Format the output as a text-based table.
                        let mut output = String::new();
                        let header = cols.join(" | ");
                        output.push_str(&header);
                        output.push('\n');
                        output.push_str(&"-".repeat(header.len()));
                        output.push('\n');

                        for row in &rows {
                            output.push_str(&row.values.join(" | "));
                            output.push('\n');
                        }
                        // Trim the final newline for a clean output.
                        output.trim_end().to_string()
                    }
                }
                Err(e) => format!("Error: {}", e),
            }
        }
        Command::ShowTables => {
            let tables = query_engine.get_all_tables();
            if tables.is_empty() {
                "No tables in database".to_string()
            } else {
                let mut output = "Tables:\n".to_string();
                for table in tables {
                    output.push_str(&format!("- {}\n", table.name));
                }
                output.trim_end().to_string()
            }
        }
        Command::InspectTable { name } => {
            if let Some(table) = query_engine.get_table_schema(&name) {
                let mut output = format!("Table: {}\n", name);
                output.push_str("----------------\n");
                output.push_str(&format!("{:<20} | {}\n", "Column", "Type"));
                output.push_str(&format!("{:-<20}-+-{:-<15}\n", "", ""));
                
                for column in &table.columns {
                    output.push_str(&format!("{:<20} | {}\n", column.name, column.data_type));
                }
                output.trim_end().to_string()
            } else {
                format!("Table '{}' not found", name)
            }
        }
        Command::Update { table, set_column, set_value, where_clause } => {
            match query_engine.execute_update(table.clone(), (set_column, set_value), where_clause) {
                Ok(count) => format!("Updated {} rows in '{}'", count, table),
                Err(e) => format!("Error: {}", e),
            }
        }
        Command::Unknown(cmd) => {
            format!("Unknown command: {}\nType 'help' for available commands", cmd)
        }
    }
}

/// Returns a help string with available commands.
///
/// This is a helper function to avoid cluttering the main execution logic.
fn print_help() -> String {
    "Available commands:\n".to_owned() +
    "  CREATE TABLE <table_name> (col1 TYPE, col2 TYPE, ...) - Create a new table\n" +
    "  INSERT INTO <table_name> VALUES (val1, val2, ...) - Insert data into a table\n" +
    "  SELECT * FROM <table_name> - Query data from a table\n" +
    "  SELECT * FROM <table_name> WHERE <column> = <value> or <column> != <value> - Query data with a where clause\n" +
    "  UPDATE <table_name> SET <column> = <value> WHERE <column> = <value> or <column> != <value> - Update data in a table\n" +
    "  INSPECT <table_name> - Show table schema and column types\n" +
    "  SHOW TABLES - List all tables in the database\n" +
    "  help - Show this help message\n" +
    "  exit | quit - Exit the program"
}
