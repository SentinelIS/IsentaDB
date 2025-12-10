mod storage;
mod parser;
mod engine;
mod database;

use std::io::{self, Write};
use database::Database;
use parser::{Command, Parser};
use engine::Catalog;

fn main() {
    println!("IsentaDB v0.1.0");
    println!("Type 'help' for commands, 'exit' to quit\n");

    // Initialize database
    let mut db = match Database::new("data.db") {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Failed to initialize database: {}", e);
            return;
        }
    };
    
    // Load catalog into memory
    let mut catalog = match db.load_catalog() {
        Ok(cat) => cat,
        Err(e) => {
            eprintln!("Failed to load catalog: {}", e);
            Catalog::new()
        }
    };
    
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
                // Check if table already exists
                if catalog.find_table(&name).is_some() {
                    println!("Error: Table '{}' already exists", name);
                    continue;
                }
                
                // Create a new table in the database
                let table = engine::Table {
                    name: name.clone(),
                    columns: columns.clone(),
                    rows: Vec::new(),
                };
                
                match db.save_table(&table, true) {
                    Ok(_) => {
                        // Add to in-memory catalog
                        catalog.add_table(table);
                        println!("Table '{}' created successfully", name);
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
            Command::Insert { table, values } => {
                if let Some(table_data) = catalog.find_table_mut(&table) {
                    // Validate column count
                    if values.len() != table_data.columns.len() {
                        println!("Error: Column count mismatch: expected {}, got {}", 
                                table_data.columns.len(), values.len());
                        continue;
                    }
                    
                    // Create a new row and add it to the table
                    let row = engine::Row { values: values.clone() };
                    table_data.rows.push(row);
                    
                    // Save the updated table
                    match db.update_table_data(table_data) {
                        Ok(_) => println!("Inserted 1 row into '{}'", table),
                        Err(e) => println!("Error: {}", e),
                    }
                } else {
                    println!("Table '{}' not found", table);
                }
            }
            Command::Select { table, columns: _ } => {
                if let Some(table_data) = catalog.find_table(&table) {
                    if table_data.rows.is_empty() {
                        println!("No rows found in '{}'", table);
                    } else {
                        // Print header
                        let header: Vec<String> = table_data
                            .columns
                            .iter()
                            .map(|c| format!("{} ({})", c.name, c.data_type))
                            .collect();
                        println!("{}", header.join(" | "));
                        println!("{}", "-".repeat(header.join(" | ").len()));

                        // Print rows
                        for row in &table_data.rows {
                            println!("{}", row.values.join(" | "));
                        }
                    }
                } else {
                    println!("Table '{}' not found", table);
                }
            }

            Command::ShowTables => {
                let tables = catalog.list_tables();
                if tables.is_empty() {
                    println!("No tables in database");
                } else {
                    println!("Tables:");
                    for table in tables {
                        println!("- {}", table);
                    }
                }
            }
            
            Command::InspectTable { name } => {
                if let Some(table) = catalog.find_table(&name) {
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
    println!("  INSPECT <table_name> - Show table schema and column types");
    println!("  SHOW TABLES - List all tables in the database");
    println!("  help - Show this help message");
    println!("  exit | quit - Exit the program");
}