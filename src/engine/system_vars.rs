// System variables (similar to MSSQL @@VARIABLES)
use crate::functions::Value;
use std::collections::HashMap;

pub struct SystemVariables {
    variables: HashMap<String, Value>,
}

impl SystemVariables {
    pub fn new() -> Self {
        let mut variables = HashMap::new();

        // Static system variables
        variables.insert("VERSION".to_string(), Value::String(env!("CARGO_PKG_VERSION").to_string()));
        variables.insert("DB_NAME".to_string(), Value::String("DDB".to_string()));
        variables.insert("DB_TYPE".to_string(), Value::String("Flat File Database".to_string()));

        // Runtime variables (initialized with defaults)
        variables.insert("ROWS_SCANNED".to_string(), Value::Integer(0));
        variables.insert("ROWS_RETURNED".to_string(), Value::Integer(0));
        variables.insert("LAST_ERROR".to_string(), Value::Null);

        SystemVariables { variables }
    }

    /// Get a system variable value
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.variables.get(&name.to_uppercase())
    }

    /// Set a system variable value (for runtime variables)
    pub fn set(&mut self, name: &str, value: Value) {
        self.variables.insert(name.to_uppercase(), value);
    }

    /// Get all available system variable names
    pub fn list_variables(&self) -> Vec<String> {
        let mut vars: Vec<_> = self.variables.keys().cloned().collect();
        vars.sort();
        vars
    }
}

impl Default for SystemVariables {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_variables() {
        let sys_vars = SystemVariables::new();

        // Test static variables
        assert!(sys_vars.get("VERSION").is_some());
        assert!(sys_vars.get("DB_NAME").is_some());

        // Test case insensitivity
        assert_eq!(sys_vars.get("VERSION"), sys_vars.get("version"));
    }

    #[test]
    fn test_set_variable() {
        let mut sys_vars = SystemVariables::new();

        sys_vars.set("ROWS_SCANNED", Value::Integer(42));
        assert_eq!(sys_vars.get("ROWS_SCANNED"), Some(&Value::Integer(42)));
    }

    #[test]
    fn test_list_variables() {
        let sys_vars = SystemVariables::new();
        let vars = sys_vars.list_variables();

        assert!(vars.contains(&"VERSION".to_string()));
        assert!(vars.contains(&"DB_NAME".to_string()));
        assert!(vars.contains(&"ROWS_SCANNED".to_string()));
    }
}
