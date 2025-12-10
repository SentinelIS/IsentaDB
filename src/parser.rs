#[derive(Debug, PartialEq)]
pub enum Command {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table: String,
        values: Vec<String>,
    },
    Select {
        table: String,
        columns: Vec<String>, // For now, just support *
    },
    ShowTables,
    InspectTable {
        name: String,
    },
    Unknown(String),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

pub struct Parser {}

impl Parser {
    pub fn new() -> Self {
        Parser {}
    }

    pub fn parse(&self, input: &str) -> Command {
        let input = input.trim();
        let input_upper = input.to_uppercase();

        if input_upper.starts_with("CREATE TABLE") {
            self.parse_create_table(input)
        } else if input_upper.starts_with("INSERT INTO") {
            self.parse_insert(input)
        } else if input_upper.starts_with("SELECT") {
            self.parse_select(input)
        } else if input_upper.starts_with("SHOW TABLES") {
            Command::ShowTables
        } else if input_upper.starts_with("INSPECT") {
            self.parse_inspect(input)
        } else {
            Command::Unknown(input.to_string())
        }
    }

    fn parse_create_table(&self, input: &str) -> Command {
        // Format: CREATE TABLE name (col1 TYPE, col2 TYPE)
        let input_upper = input.to_uppercase();
        let rest = match input_upper.strip_prefix("CREATE TABLE") {
            Some(r) => r.trim(),
            None => return Command::Unknown(input.to_string()),
        };

        // Find the opening parenthesis
        let parts: Vec<&str> = rest.splitn(2, '(').collect();
        if parts.len() != 2 {
            return Command::Unknown(input.to_string());
        }

        let table_name = parts[0].trim().to_string();
        let columns_str = parts[1].trim_end_matches(')').trim();

        // Parse columns: "col1 TYPE, col2 TYPE"
        let columns: Vec<Column> = columns_str
            .split(',')
            .filter_map(|col| {
                let parts: Vec<&str> = col.trim().split_whitespace().collect();
                if parts.len() >= 2 {
                    Some(Column {
                        name: parts[0].to_string(),
                        data_type: parts[1].to_uppercase(),
                    })
                } else if parts.len() == 1 && !parts[0].is_empty() {
                    // Default to TEXT if no type specified
                    Some(Column {
                        name: parts[0].to_string(),
                        data_type: "TEXT".to_string(),
                    })
                } else {
                    None
                }
            })
            .collect();

        Command::CreateTable {
            name: table_name,
            columns,
        }
    }

    fn parse_insert(&self, input: &str) -> Command {
        // Format: INSERT INTO table VALUES (val1, val2)
        let input_upper = input.to_uppercase();
        if !input_upper.starts_with("INSERT INTO") {
            return Command::Unknown(input.to_string());
        }

        // Find VALUES keyword position (case-insensitive) in original input
        let after_insert = &input[11..].trim_start(); // Skip "INSERT INTO" (11 chars)
        let values_pos_original = match after_insert.to_uppercase().find("VALUES") {
            Some(pos) => pos,
            None => return Command::Unknown(input.to_string()),
        };
        
        let table_name = after_insert[..values_pos_original].trim().to_string();
        let values_str = after_insert[values_pos_original + 6..].trim().trim_start_matches('(').trim_end_matches(')');

        // Parse values - simple split by comma
        let values: Vec<String> = values_str
            .split(',')
            .map(|v| v.trim().trim_matches('\'').trim_matches('"').to_string())
            .collect();

        Command::Insert {
            table: table_name,
            values,
        }
    }

    fn parse_select(&self, input: &str) -> Command {
        // Format: SELECT * FROM table
        let input_upper = input.to_uppercase();
        if !input_upper.starts_with("SELECT") {
            return Command::Unknown(input.to_string());
        }

        // Find FROM keyword position (case-insensitive) in original input
        let after_select = &input[6..].trim_start(); // Skip "SELECT" (6 chars)
        let from_pos_original = match after_select.to_uppercase().find("FROM") {
            Some(pos) => pos,
            None => return Command::Unknown(input.to_string()),
        };
        
        let after_from = &after_select[from_pos_original + 4..].trim_start();
        let columns_str = after_select[..from_pos_original].trim();
        let table_name = after_from.to_string();

        // Parse columns (for now, just support *)
        let columns: Vec<String> = if columns_str == "*" {
            vec!["*".to_string()]
        } else {
            columns_str
                .split(',')
                .map(|c| c.trim().to_string())
                .collect()
        };

        Command::Select {
            table: table_name,
            columns,
        }
    }

    fn parse_inspect(&self, input: &str) -> Command {
        let input_upper = input.to_uppercase();
        let rest = match input_upper.strip_prefix("INSPECT") {
            Some(r) => r.trim(),
            None => return Command::Unknown(input.to_string()),
        };

        if rest.is_empty() {
            return Command::Unknown(input.to_string());
        }

        Command::InspectTable {
            name: rest.to_string(),
        }
    }
}