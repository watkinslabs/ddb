// Function registry - maps function names to implementations
use super::Value;
use crate::error::{DdbError, Result};
use std::collections::HashMap;

/// Function signature types
#[derive(Debug, Clone)]
pub enum FunctionSignature {
    /// Function with no arguments
    Nullary(fn() -> Value),
    /// Function with one argument
    Unary(fn(&Value) -> Result<Value>),
    /// Function with two arguments
    Binary(fn(&Value, &Value) -> Result<Value>),
    /// Function with three arguments
    Ternary(fn(&Value, &Value, &Value) -> Result<Value>),
    /// Function with variable arguments
    Variadic(fn(&[Value]) -> Result<Value>),
    /// Function with optional second argument
    UnaryOptional(fn(&Value, Option<&Value>) -> Result<Value>),
    /// Aggregate function (operates on multiple values)
    Aggregate(fn(&[Value]) -> Result<Value>),
}

/// Function metadata
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub signature: FunctionSignature,
    pub description: String,
    pub category: FunctionCategory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionCategory {
    Math,
    String,
    Conversion,
    DateTime,
    System,
    Conditional,
    Aggregate,
}

/// Global function registry
pub struct FunctionRegistry {
    functions: HashMap<String, FunctionInfo>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };
        registry.register_all();
        registry
    }

    /// Register a function
    pub fn register(&mut self, info: FunctionInfo) {
        let name = info.name.to_uppercase();
        self.functions.insert(name, info);
    }

    /// Get function by name
    pub fn get(&self, name: &str) -> Option<&FunctionInfo> {
        self.functions.get(&name.to_uppercase())
    }

    /// List all functions
    pub fn list(&self) -> Vec<&FunctionInfo> {
        self.functions.values().collect()
    }

    /// List functions by category
    pub fn list_by_category(&self, category: FunctionCategory) -> Vec<&FunctionInfo> {
        self.functions
            .values()
            .filter(|f| f.category == category)
            .collect()
    }

    /// Register all built-in functions
    fn register_all(&mut self) {
        // Math functions
        self.register(FunctionInfo {
            name: "ABS".to_string(),
            signature: FunctionSignature::Unary(super::math::abs),
            description: "Absolute value".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "CEIL".to_string(),
            signature: FunctionSignature::Unary(super::math::ceil),
            description: "Round up to nearest integer".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "FLOOR".to_string(),
            signature: FunctionSignature::Unary(super::math::floor),
            description: "Round down to nearest integer".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "SQRT".to_string(),
            signature: FunctionSignature::Unary(super::math::sqrt),
            description: "Square root".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "POW".to_string(),
            signature: FunctionSignature::Binary(super::math::pow),
            description: "Power (x^y)".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "EXP".to_string(),
            signature: FunctionSignature::Unary(super::math::exp),
            description: "e^x".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "LN".to_string(),
            signature: FunctionSignature::Unary(super::math::ln),
            description: "Natural logarithm".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "LOG10".to_string(),
            signature: FunctionSignature::Unary(super::math::log10),
            description: "Base-10 logarithm".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "MOD".to_string(),
            signature: FunctionSignature::Binary(super::math::modulo),
            description: "Modulo operation".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "SIGN".to_string(),
            signature: FunctionSignature::Unary(super::math::sign),
            description: "Sign of number (-1, 0, 1)".to_string(),
            category: FunctionCategory::Math,
        });

        self.register(FunctionInfo {
            name: "PI".to_string(),
            signature: FunctionSignature::Nullary(super::math::pi),
            description: "Pi constant".to_string(),
            category: FunctionCategory::Math,
        });

        // String functions
        self.register(FunctionInfo {
            name: "CONCAT".to_string(),
            signature: FunctionSignature::Variadic(super::string::concat),
            description: "Concatenate strings".to_string(),
            category: FunctionCategory::String,
        });

        self.register(FunctionInfo {
            name: "UPPER".to_string(),
            signature: FunctionSignature::Unary(super::string::upper),
            description: "Convert to uppercase".to_string(),
            category: FunctionCategory::String,
        });

        self.register(FunctionInfo {
            name: "LOWER".to_string(),
            signature: FunctionSignature::Unary(super::string::lower),
            description: "Convert to lowercase".to_string(),
            category: FunctionCategory::String,
        });

        self.register(FunctionInfo {
            name: "LENGTH".to_string(),
            signature: FunctionSignature::Unary(super::string::length),
            description: "String length".to_string(),
            category: FunctionCategory::String,
        });

        self.register(FunctionInfo {
            name: "TRIM".to_string(),
            signature: FunctionSignature::Unary(super::string::trim),
            description: "Remove whitespace".to_string(),
            category: FunctionCategory::String,
        });

        self.register(FunctionInfo {
            name: "REVERSE".to_string(),
            signature: FunctionSignature::Unary(super::string::reverse),
            description: "Reverse string".to_string(),
            category: FunctionCategory::String,
        });

        // Conversion functions
        self.register(FunctionInfo {
            name: "ATOF".to_string(),
            signature: FunctionSignature::Unary(super::conversion::atof),
            description: "ASCII to float".to_string(),
            category: FunctionCategory::Conversion,
        });

        self.register(FunctionInfo {
            name: "ATOI".to_string(),
            signature: FunctionSignature::Unary(super::conversion::atoi),
            description: "ASCII to integer".to_string(),
            category: FunctionCategory::Conversion,
        });

        self.register(FunctionInfo {
            name: "HEX".to_string(),
            signature: FunctionSignature::Unary(super::conversion::hex),
            description: "Convert to hexadecimal".to_string(),
            category: FunctionCategory::Conversion,
        });

        self.register(FunctionInfo {
            name: "BIN".to_string(),
            signature: FunctionSignature::Unary(super::conversion::bin),
            description: "Convert to binary".to_string(),
            category: FunctionCategory::Conversion,
        });

        // Date/Time functions
        self.register(FunctionInfo {
            name: "NOW".to_string(),
            signature: FunctionSignature::Nullary(super::datetime::now),
            description: "Current date and time".to_string(),
            category: FunctionCategory::DateTime,
        });

        self.register(FunctionInfo {
            name: "CURDATE".to_string(),
            signature: FunctionSignature::Nullary(super::datetime::curdate),
            description: "Current date".to_string(),
            category: FunctionCategory::DateTime,
        });

        self.register(FunctionInfo {
            name: "CURTIME".to_string(),
            signature: FunctionSignature::Nullary(super::datetime::curtime),
            description: "Current time".to_string(),
            category: FunctionCategory::DateTime,
        });

        // System functions
        self.register(FunctionInfo {
            name: "VERSION".to_string(),
            signature: FunctionSignature::Nullary(super::system::version),
            description: "DDB version".to_string(),
            category: FunctionCategory::System,
        });

        self.register(FunctionInfo {
            name: "UUID".to_string(),
            signature: FunctionSignature::Nullary(super::system::uuid),
            description: "Generate UUID".to_string(),
            category: FunctionCategory::System,
        });

        // Conditional functions
        self.register(FunctionInfo {
            name: "IF".to_string(),
            signature: FunctionSignature::Ternary(super::conditional::if_fn),
            description: "Conditional expression".to_string(),
            category: FunctionCategory::Conditional,
        });

        self.register(FunctionInfo {
            name: "IFNULL".to_string(),
            signature: FunctionSignature::Binary(super::conditional::ifnull),
            description: "Return alt if NULL".to_string(),
            category: FunctionCategory::Conditional,
        });

        self.register(FunctionInfo {
            name: "NULLIF".to_string(),
            signature: FunctionSignature::Binary(super::conditional::nullif),
            description: "Return NULL if equal".to_string(),
            category: FunctionCategory::Conditional,
        });

        self.register(FunctionInfo {
            name: "COALESCE".to_string(),
            signature: FunctionSignature::Variadic(super::conditional::coalesce),
            description: "First non-NULL value".to_string(),
            category: FunctionCategory::Conditional,
        });

        // Aggregate functions
        self.register(FunctionInfo {
            name: "MAX".to_string(),
            signature: FunctionSignature::Aggregate(super::aggregate::max),
            description: "Maximum value".to_string(),
            category: FunctionCategory::Aggregate,
        });

        self.register(FunctionInfo {
            name: "MIN".to_string(),
            signature: FunctionSignature::Aggregate(super::aggregate::min),
            description: "Minimum value".to_string(),
            category: FunctionCategory::Aggregate,
        });
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry() {
        let registry = FunctionRegistry::new();

        assert!(registry.get("ABS").is_some());
        assert!(registry.get("CONCAT").is_some());
        assert!(registry.get("NOW").is_some());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_category_filter() {
        let registry = FunctionRegistry::new();
        let math_funcs = registry.list_by_category(FunctionCategory::Math);
        assert!(!math_funcs.is_empty());
    }
}
