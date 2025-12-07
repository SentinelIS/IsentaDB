use crate::parser::Column;

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<String>,
}

pub struct Catalog {
    tables: Vec<Table>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: Vec::new(),
        }
    }

    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), String> {
        // Check if table already exists
        if self.tables.iter().any(|t| t.name == name) {
            return Err(format!("Table '{}' already exists", name));
        }

        let table = Table {
            name,
            columns,
            rows: Vec::new(),
        };
        self.tables.push(table);
        Ok(())
    }

    pub fn find_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.iter_mut().find(|t| t.name.to_lowercase() == name.to_lowercase())
    }

    pub fn find_table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name.to_lowercase() == name.to_lowercase())
    }

    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.iter().map(|t| t.name.as_str()).collect()
    }

    pub fn get_all_tables(&self) -> &Vec<Table> {
        &self.tables
    }

    pub fn load_tables(&mut self, tables: Vec<Table>) {
        for table in tables {
            if !self.tables.iter().any(|t| t.name == table.name) {
                self.tables.push(table);
            }
        }
    }
    
    pub fn add_table(&mut self, table: Table) {
        if !self.tables.iter().any(|t| t.name.to_lowercase() == table.name.to_lowercase()) {
            self.tables.push(table);
        }
    }
}

pub struct QueryEngine {
    catalog: Catalog,
    database: crate::database::Database,
}

impl QueryEngine {
    pub fn new() -> Self {
        Self::with_database("data.db")
    }

    pub fn with_database(path: &str) -> Self {
        let mut database = crate::database::Database::new(path)
            .expect("Failed to initialize database");
        
        let catalog = database.load_catalog()
            .unwrap_or_else(|e| {
                eprintln!("Warning: Failed to load catalog: {}. Starting with empty database.", e);
                Catalog::new()
            });

        QueryEngine {
            catalog,
            database,
        }
    }

    pub fn execute_create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), String> {
        self.catalog.create_table(name.clone(), columns.clone())?;
        
        // Get the table we just created and save it to disk
        let table = self.catalog.find_table(&name)
            .ok_or_else(|| format!("Failed to find table '{}' after creation", name))?
            .clone();
        
        self.database.save_table(&table, true)?;
        Ok(())
    }

    pub fn execute_insert(&mut self, table: String, values: Vec<String>) -> Result<(), String> {
        let table_ref = self
            .catalog
            .find_table_mut(&table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;

        // Validate column count
        if values.len() != table_ref.columns.len() {
            return Err(format!(
                "Column count mismatch: expected {}, got {}",
                table_ref.columns.len(),
                values.len()
            ));
        }

        table_ref.rows.push(Row { values });
        
        // Save updated table to disk
        let table_clone = table_ref.clone();
        self.database.update_table_data(&table_clone)?;
        Ok(())
    }

    pub fn execute_select(&self, table: String) -> Result<Vec<Row>, String> {
        let table = self
            .catalog
            .find_table(&table)
            .ok_or_else(|| format!("Table '{}' does not exist", table))?;

        Ok(table.rows.clone())
    }

    pub fn get_table_schema(&self, table: &str) -> Option<&Table> {
        self.catalog.find_table(table)
    }
}