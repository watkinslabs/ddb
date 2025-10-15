// CSV/Delimited file reader
use crate::engine::row::Row;
use crate::error::{DdbError, Result};
use crate::functions::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct CsvReader {
    reader: BufReader<File>,
    delimiter: char,
    column_names: Vec<String>,
    line_number: usize,
    has_header: bool,
}

impl CsvReader {
    /// Create a new CSV reader with specified delimiter
    pub fn new<P: AsRef<Path>>(path: P, delimiter: char, has_header: bool) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        Ok(Self {
            reader,
            delimiter,
            column_names: Vec::new(),
            line_number: 0,
            has_header,
        })
    }

    /// Create a CSV reader with explicit column names (no header in file)
    pub fn with_columns<P: AsRef<Path>>(
        path: P,
        delimiter: char,
        columns: Vec<String>,
    ) -> Result<Self> {
        let mut reader = Self::new(path, delimiter, false)?;
        reader.column_names = columns;
        Ok(reader)
    }

    /// Get the column names
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Read the next row
    pub fn next_row(&mut self) -> Result<Option<Row>> {
        let mut line = String::new();

        match self.reader.read_line(&mut line) {
            Ok(0) => Ok(None), // EOF
            Ok(_) => {
                self.line_number += 1;
                line = line.trim_end_matches(&['\r', '\n'][..]).to_string();

                // If this is the first line and we have a header, parse column names
                if self.line_number == 1 && self.has_header {
                    self.column_names = self.parse_line(&line);
                    return self.next_row(); // Read next line for actual data
                }

                // If we don't have column names, generate them
                if self.column_names.is_empty() {
                    let values = self.parse_line(&line);
                    self.column_names = (0..values.len())
                        .map(|i| format!("col{}", i))
                        .collect();
                }

                let values = self.parse_line(&line);
                let row = self.create_row(values)?;
                Ok(Some(row))
            }
            Err(e) => Err(DdbError::IoError(e)),
        }
    }

    /// Parse a line into fields
    fn parse_line(&self, line: &str) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '"' {
                in_quotes = !in_quotes;
            } else if ch == self.delimiter && !in_quotes {
                fields.push(current_field.clone());
                current_field.clear();
            } else {
                current_field.push(ch);
            }
        }

        // Add the last field
        fields.push(current_field);

        fields
    }

    /// Create a Row from parsed field values
    fn create_row(&self, fields: Vec<String>) -> Result<Row> {
        if fields.len() != self.column_names.len() {
            return Err(DdbError::InvalidColumnCount {
                expected: self.column_names.len(),
                actual: fields.len(),
            });
        }

        let mut row = Row::new();
        for (name, field) in self.column_names.iter().zip(fields.iter()) {
            let value = Value::from_str(field);
            row.set(name, value);
        }

        Ok(row)
    }

    /// Get current line number
    pub fn line_number(&self) -> usize {
        self.line_number
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_csv_reader_with_header() {
        // Create a temporary CSV file
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "id,name,age").unwrap();
        writeln!(file, "1,Alice,30").unwrap();
        writeln!(file, "2,Bob,25").unwrap();
        file.flush().unwrap();

        let mut reader = CsvReader::new(file.path(), ',', true).unwrap();

        // Read first row
        let row1 = reader.next_row().unwrap().unwrap();
        assert_eq!(row1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(row1.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(row1.get("age"), Some(&Value::Integer(30)));

        // Read second row
        let row2 = reader.next_row().unwrap().unwrap();
        assert_eq!(row2.get("id"), Some(&Value::Integer(2)));

        // EOF
        assert!(reader.next_row().unwrap().is_none());
    }

    #[test]
    fn test_csv_reader_without_header() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "1,Alice,30").unwrap();
        writeln!(file, "2,Bob,25").unwrap();
        file.flush().unwrap();

        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let mut reader = CsvReader::with_columns(file.path(), ',', columns).unwrap();

        let row = reader.next_row().unwrap().unwrap();
        assert_eq!(row.get("id"), Some(&Value::Integer(1)));
        assert_eq!(row.get("name"), Some(&Value::String("Alice".to_string())));
    }

    #[test]
    fn test_csv_reader_quoted_fields() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,description").unwrap();
        writeln!(file, r#"Test,"A field, with comma""#).unwrap();
        file.flush().unwrap();

        let mut reader = CsvReader::new(file.path(), ',', true).unwrap();
        let row = reader.next_row().unwrap().unwrap();

        assert_eq!(row.get("name"), Some(&Value::String("Test".to_string())));
        assert_eq!(row.get("description"), Some(&Value::String("A field, with comma".to_string())));
    }
}
