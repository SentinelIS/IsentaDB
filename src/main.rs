mod storage;
mod parser;
mod engine;
mod database;
mod wal;

use std::io::{self, Write};
use parser::{Command, Parser};
use engine::QueryEngine;

fn main() {
    println!("IsentaDB v0.1.0");
    println!("Type 'help' for commands, 'exit' to quit\n");

    // Initialize the query engine
    let mut query_engine = QueryEngine::new();
    
    let parser = Parser::new();

    // Simple REPL loop
    loop {
        print!("isenta> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {}
            Err(_) => {
                println!("Error reading input");
                continue;
            }
        }

        let input = input.trim();

        // Skip empty input
        if input.is_empty() {
            continue;
        }

        // Handle special commands
        match input.to_lowercase().as_str() {
            "exit" | "quit" => {
                println!("Goodbye!");
                break;
            }
            "help" => {
                print_help();
                continue;
            }
            _ => {}
        }

        // Parse and execute SQL command
        let command = parser.parse(input);
        match command {
            Command::CreateTable { name, columns } => {
                match query_engine.execute_create_table(name.clone(), columns) {
                    Ok(_) => println!("Table '{}' created successfully", name),
                    Err(e) => println!("Error: {}", e),
                }
            }
            Command::Insert { table, values } => {
                match query_engine.execute_insert(table.clone(), values) {
                    Ok(_) => println!("Inserted 1 row into '{}'", table),
                    Err(e) => println!("Error: {}", e),
                }
            }
            Command::Select { table, columns, where_clause } => {
                match query_engine.execute_select(table.clone(), columns, where_clause) {
                    Ok((cols, rows)) => {
                        if rows.is_empty() {
                            println!("No rows found in '{}'", table);
                        } else {
                            // Print header
                            println!("{}", cols.join(" | "));
                            println!("{}", "-".repeat(cols.join(" | ").len()));

                            // Print rows
                            for row in &rows {
                                println!("{}", row.values.join(" | "));
                            }
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            Command::ShowTables => {
                if let Some(schema) = query_engine.get_table_schema("tables") {
                    if schema.rows.is_empty() {
                        println!("No tables in database");
                    } else {
                        println!("Tables:");
                        for row in &schema.rows {
                            println!("- {}", row.values.join(" | "));
                        }
                    }
                } else {
                    let tables = query_engine.get_all_tables();
                    if tables.is_empty() {
                        println!("No tables in database");
                    } else {
                        println!("Tables:");
                        for table in tables {
                            println!("- {}", table.name);
                        }
                    }
                }
            }
            
            Command::InspectTable { name } => {
                if let Some(table) = query_engine.get_table_schema(&name) {
                    println!("Table: {}", name);
                    println!("----------------");
                    println!("{:<20} | {}", "Column", "Type");
                    println!("{:-<20}-+-{:-<15}", "", "");
                    
                    for column in &table.columns {
                        println!("{:<20} | {}", column.name, column.data_type);
                    }
                } else {
                    println!("Table '{}' not found", name);
                }
            }

            Command::Unknown(cmd) => {
                println!("Unknown command: {}", cmd);
                println!("Type 'help' for available commands");
            }
        }
    }
}

fn print_help() {
    println!("Available commands:");
    println!("  CREATE TABLE <table_name> (col1 TYPE, col2 TYPE, ...) - Create a new table");
    println!("  INSERT INTO <table_name> VALUES (val1, val2, ...) - Insert data into a table");
    println!("  SELECT * FROM <table_name> - Query data from a table");
    println!("  SELECT * FROM <table_name> WHERE <column> = <value> - Query data with a where clause");
    println!("  INSPECT <table_name> - Show table schema and column types");
    println!("  SHOW TABLES - List all tables in the database");
    println!("  help - Show this help message");
    println!("  exit | quit - Exit the program");
}