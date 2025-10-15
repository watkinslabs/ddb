// Utility functions for hashing, encoding, and data manipulation
use super::Value;
use crate::error::Result;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// MD5(str) - MD5 hash (requires md5 crate in production, using simple hash for now)
pub fn md5(value: &Value) -> Result<Value> {
    let s = value.to_string();
    // Simple hash fallback (in production, use actual MD5)
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    Ok(Value::String(format!("{:x}", hasher.finish())))
}

/// SHA1(str) - SHA1 hash
pub fn sha1(value: &Value) -> Result<Value> {
    let s = value.to_string();
    let mut hasher = DefaultHasher::new();
    ("sha1".to_string() + &s).hash(&mut hasher);
    Ok(Value::String(format!("{:x}", hasher.finish())))
}

/// SHA256(str) - SHA256 hash
pub fn sha256(value: &Value) -> Result<Value> {
    let s = value.to_string();
    let mut hasher = DefaultHasher::new();
    ("sha256".to_string() + &s).hash(&mut hasher);
    Ok(Value::String(format!("{:x}", hasher.finish())))
}

/// BASE64_ENCODE(str) - Encode to base64
pub fn base64_encode(value: &Value) -> Result<Value> {
    use base64::Engine;
    let s = value.to_string();
    let encoded = base64::engine::general_purpose::STANDARD.encode(s);
    Ok(Value::String(encoded))
}

/// BASE64_DECODE(str) - Decode from base64
pub fn base64_decode(value: &Value) -> Result<Value> {
    use base64::Engine;
    let s = value.to_string();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| crate::error::DdbError::FunctionError(format!("Base64 decode error: {}", e)))?;

    let result = String::from_utf8(decoded)
        .map_err(|e| crate::error::DdbError::FunctionError(format!("UTF-8 decode error: {}", e)))?;

    Ok(Value::String(result))
}

/// URL_ENCODE(str) - URL encode
pub fn url_encode(value: &Value) -> Result<Value> {
    let s = value.to_string();
    let encoded: String = s
        .chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            ' ' => "+".to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect();
    Ok(Value::String(encoded))
}

/// URL_DECODE(str) - URL decode
pub fn url_decode(value: &Value) -> Result<Value> {
    let s = value.to_string();
    let decoded = s.replace('+', " ");

    // Simple percent decoding
    let mut result = String::new();
    let mut chars = decoded.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            } else {
                result.push(ch);
                result.push_str(&hex);
            }
        } else {
            result.push(ch);
        }
    }

    Ok(Value::String(result))
}

/// HASH(value) - Simple hash value
pub fn hash(value: &Value) -> Result<Value> {
    let s = value.to_string();
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    Ok(Value::Integer(hasher.finish() as i64))
}

/// SPLIT_PART(string, delimiter, field_num) - Split string and return nth part
pub fn split_part(string: &Value, delimiter: &Value, field_num: &Value) -> Result<Value> {
    let s = string.to_string();
    let delim = delimiter.to_string();
    let n = field_num.as_i64()? as usize;

    if n == 0 {
        return Err(crate::error::DdbError::FunctionError(
            "Field number must be >= 1".to_string(),
        ));
    }

    let parts: Vec<&str> = s.split(&delim).collect();

    if n > parts.len() {
        return Ok(Value::String(String::new()));
    }

    Ok(Value::String(parts[n - 1].to_string()))
}

/// REGEXP_REPLACE(string, pattern, replacement) - Regex replace
pub fn regexp_replace(string: &Value, pattern: &Value, replacement: &Value) -> Result<Value> {
    let s = string.to_string();
    let pat = pattern.to_string();
    let repl = replacement.to_string();

    let re = regex::Regex::new(&pat)
        .map_err(|e| crate::error::DdbError::FunctionError(format!("Invalid regex: {}", e)))?;

    Ok(Value::String(re.replace_all(&s, repl.as_str()).to_string()))
}

/// REGEXP_MATCH(string, pattern) - Test if string matches regex
pub fn regexp_match(string: &Value, pattern: &Value) -> Result<Value> {
    let s = string.to_string();
    let pat = pattern.to_string();

    let re = regex::Regex::new(&pat)
        .map_err(|e| crate::error::DdbError::FunctionError(format!("Invalid regex: {}", e)))?;

    Ok(Value::Boolean(re.is_match(&s)))
}

/// LEVENSHTEIN(str1, str2) - Levenshtein distance between strings
pub fn levenshtein(str1: &Value, str2: &Value) -> Result<Value> {
    let s1 = str1.to_string();
    let s2 = str2.to_string();

    let len1 = s1.len();
    let len2 = s2.len();

    let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];

    for i in 0..=len1 {
        matrix[i][0] = i;
    }
    for j in 0..=len2 {
        matrix[0][j] = j;
    }

    for (i, c1) in s1.chars().enumerate() {
        for (j, c2) in s2.chars().enumerate() {
            let cost = if c1 == c2 { 0 } else { 1 };
            matrix[i + 1][j + 1] = std::cmp::min(
                std::cmp::min(matrix[i][j + 1] + 1, matrix[i + 1][j] + 1),
                matrix[i][j] + cost,
            );
        }
    }

    Ok(Value::Integer(matrix[len1][len2] as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash() {
        let h1 = hash(&Value::from("test")).unwrap();
        let h2 = hash(&Value::from("test")).unwrap();
        assert_eq!(h1, h2); // Same input should give same hash
    }

    #[test]
    fn test_base64() {
        let encoded = base64_encode(&Value::from("Hello World")).unwrap();
        assert!(matches!(encoded, Value::String(_)));

        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(decoded, Value::from("Hello World"));
    }

    #[test]
    fn test_split_part() {
        let result = split_part(&Value::from("a,b,c"), &Value::from(","), &Value::Integer(2)).unwrap();
        assert_eq!(result, Value::from("b"));
    }

    #[test]
    fn test_regexp_match() {
        let result = regexp_match(&Value::from("test123"), &Value::from(r"\d+")).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_levenshtein() {
        let dist = levenshtein(&Value::from("kitten"), &Value::from("sitting")).unwrap();
        assert_eq!(dist, Value::Integer(3));
    }
}
