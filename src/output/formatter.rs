// Output formatting for query results
use crate::engine::Row;
use crate::error::Result;
use prettytable::{Cell, Row as PrettyRow, Table};

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Table,
    Json,
    Yaml,
    Csv,
    Xml,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => OutputFormat::Json,
            "yaml" => OutputFormat::Yaml,
            "csv" => OutputFormat::Csv,
            "xml" => OutputFormat::Xml,
            _ => OutputFormat::Table,
        }
    }
}

/// Format query results according to the specified output format
pub fn format_results(rows: &[Row], format: OutputFormat) -> Result<String> {
    if rows.is_empty() {
        return Ok("No results".to_string());
    }

    match format {
        OutputFormat::Table => format_table(rows),
        OutputFormat::Json => format_json(rows),
        OutputFormat::Yaml => format_yaml(rows),
        OutputFormat::Csv => format_csv(rows),
        OutputFormat::Xml => format_xml(rows),
    }
}

/// Format results as a pretty ASCII table
fn format_table(rows: &[Row]) -> Result<String> {
    if rows.is_empty() {
        return Ok("No results".to_string());
    }

    let mut table = Table::new();

    // Get column names from first row
    let columns: Vec<String> = rows[0].column_names();

    // Add header row
    let header: Vec<Cell> = columns.iter().map(|c| Cell::new(c)).collect();
    table.add_row(PrettyRow::new(header));

    // Add data rows
    for row in rows {
        let values = row.values(&columns);
        let cells: Vec<Cell> = values.iter().map(|v| Cell::new(&v.to_string())).collect();
        table.add_row(PrettyRow::new(cells));
    }

    Ok(table.to_string())
}

/// Format results as JSON
fn format_json(rows: &[Row]) -> Result<String> {
    let json_rows: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (key, value) in row.as_map() {
                map.insert(key.clone(), value_to_json(value));
            }
            serde_json::Value::Object(map)
        })
        .collect();

    serde_json::to_string_pretty(&json_rows)
        .map_err(|e| crate::error::DdbError::SerializationError(e.to_string()))
}

/// Format results as YAML
fn format_yaml(rows: &[Row]) -> Result<String> {
    let yaml_rows: Vec<serde_yaml::Value> = rows
        .iter()
        .map(|row| {
            let mut map = serde_yaml::Mapping::new();
            for (key, value) in row.as_map() {
                map.insert(
                    serde_yaml::Value::String(key.clone()),
                    value_to_yaml(value),
                );
            }
            serde_yaml::Value::Mapping(map)
        })
        .collect();

    serde_yaml::to_string(&yaml_rows)
        .map_err(|e| crate::error::DdbError::SerializationError(e.to_string()))
}

/// Format results as CSV
fn format_csv(rows: &[Row]) -> Result<String> {
    if rows.is_empty() {
        return Ok(String::new());
    }

    let mut output = String::new();
    let columns: Vec<String> = rows[0].column_names();

    // Header
    output.push_str(&columns.join(","));
    output.push('\n');

    // Data rows
    for row in rows {
        let values: Vec<String> = row
            .values(&columns)
            .iter()
            .map(|v| escape_csv(&v.to_string()))
            .collect();
        output.push_str(&values.join(","));
        output.push('\n');
    }

    Ok(output)
}

/// Format results as XML
fn format_xml(rows: &[Row]) -> Result<String> {
    if rows.is_empty() {
        return Ok("<results></results>".to_string());
    }

    let mut output = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<results>\n");

    for row in rows {
        output.push_str("  <row>\n");
        for (key, value) in row.as_map() {
            // Escape XML special characters in tag name
            let safe_key = escape_xml_tag(key);
            let value_str = escape_xml(&value_to_string(value));
            output.push_str(&format!("    <{}>{}</{}>\n", safe_key, value_str, safe_key));
        }
        output.push_str("  </row>\n");
    }

    output.push_str("</results>");
    Ok(output)
}

/// Escape XML tag name (replace invalid characters with underscores)
fn escape_xml_tag(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

/// Escape XML special characters in content
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Convert Value to string for XML
fn value_to_string(value: &crate::functions::Value) -> String {
    use crate::functions::Value;

    match value {
        Value::Null => String::new(),
        Value::Boolean(b) => b.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        Value::Time(t) => t.format("%H:%M:%S").to_string(),
    }
}

/// Escape CSV field if needed
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Convert Value to JSON value
fn value_to_json(value: &crate::functions::Value) -> serde_json::Value {
    use crate::functions::Value;

    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Date(d) => serde_json::Value::String(d.format("%Y-%m-%d").to_string()),
        Value::DateTime(dt) => serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
        Value::Time(t) => serde_json::Value::String(t.format("%H:%M:%S").to_string()),
    }
}

/// Convert Value to YAML value
fn value_to_yaml(value: &crate::functions::Value) -> serde_yaml::Value {
    use crate::functions::Value;

    match value {
        Value::Null => serde_yaml::Value::Null,
        Value::Boolean(b) => serde_yaml::Value::Bool(*b),
        Value::Integer(i) => serde_yaml::Value::Number((*i).into()),
        Value::Float(f) => serde_yaml::Value::Number(serde_yaml::Number::from(*f)),
        Value::String(s) => serde_yaml::Value::String(s.clone()),
        Value::Date(d) => serde_yaml::Value::String(d.format("%Y-%m-%d").to_string()),
        Value::DateTime(dt) => serde_yaml::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()),
        Value::Time(t) => serde_yaml::Value::String(t.format("%H:%M:%S").to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::Value;

    fn create_test_rows() -> Vec<Row> {
        let columns = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let values1 = vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Integer(30),
        ];
        let values2 = vec![
            Value::Integer(2),
            Value::String("Bob".to_string()),
            Value::Integer(25),
        ];

        vec![
            Row::from_values(&columns, values1),
            Row::from_values(&columns, values2),
        ]
    }

    #[test]
    fn test_format_csv() {
        let rows = create_test_rows();
        let output = format_csv(&rows).unwrap();

        // Check that all columns are present (order may vary due to HashMap)
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("age"));
        // Check data is present
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
        assert!(output.contains("30"));
        assert!(output.contains("25"));
    }

    #[test]
    fn test_format_json() {
        let rows = create_test_rows();
        let output = format_json(&rows).unwrap();

        assert!(output.contains("\"id\""));
        assert!(output.contains("\"name\""));
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }

    #[test]
    fn test_format_yaml() {
        let rows = create_test_rows();
        let output = format_yaml(&rows).unwrap();

        assert!(output.contains("id:"));
        assert!(output.contains("name:"));
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }

    #[test]
    fn test_format_xml() {
        let rows = create_test_rows();
        let output = format_xml(&rows).unwrap();

        assert!(output.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(output.contains("<results>"));
        assert!(output.contains("<row>"));
        assert!(output.contains("<id>"));
        assert!(output.contains("<name>"));
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
        assert!(output.contains("</results>"));
    }
}
