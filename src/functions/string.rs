// String functions
use super::Value;
use crate::error::Result;

/// CONCAT(str1, str2, ...) - Concatenate strings
pub fn concat(values: &[Value]) -> Result<Value> {
    let result = values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join("");
    Ok(Value::String(result))
}

/// CONCAT_WS(separator, str1, str2, ...) - Concatenate with separator
pub fn concat_ws(separator: &Value, values: &[Value]) -> Result<Value> {
    let sep = separator.to_string();
    let result = values
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(&sep);
    Ok(Value::String(result))
}

/// UPPER(str) / UCASE(str) - Convert to uppercase
pub fn upper(value: &Value) -> Result<Value> {
    Ok(Value::String(value.to_string().to_uppercase()))
}

/// LOWER(str) / LCASE(str) - Convert to lowercase
pub fn lower(value: &Value) -> Result<Value> {
    Ok(Value::String(value.to_string().to_lowercase()))
}

/// LENGTH(str) / CHAR_LENGTH(str) - String length
pub fn length(value: &Value) -> Result<Value> {
    Ok(Value::Integer(value.to_string().len() as i64))
}

/// TRIM(str) - Remove leading/trailing whitespace
pub fn trim(value: &Value) -> Result<Value> {
    Ok(Value::String(value.to_string().trim().to_string()))
}

/// LTRIM(str) - Remove leading whitespace
pub fn ltrim(value: &Value) -> Result<Value> {
    Ok(Value::String(value.to_string().trim_start().to_string()))
}

/// RTRIM(str) - Remove trailing whitespace
pub fn rtrim(value: &Value) -> Result<Value> {
    Ok(Value::String(value.to_string().trim_end().to_string()))
}

/// SUBSTR(str, pos, [len]) / SUBSTRING(str, pos, [len])
pub fn substr(value: &Value, pos: &Value, len: Option<&Value>) -> Result<Value> {
    let s = value.to_string();
    let start = (pos.as_i64()? - 1).max(0) as usize; // SQL is 1-indexed

    if start >= s.len() {
        return Ok(Value::String(String::new()));
    }

    let result = if let Some(length) = len {
        let length = length.as_i64()?.max(0) as usize;
        s.chars().skip(start).take(length).collect()
    } else {
        s.chars().skip(start).collect()
    };

    Ok(Value::String(result))
}

/// LEFT(str, len) - Return leftmost characters
pub fn left(value: &Value, len: &Value) -> Result<Value> {
    let s = value.to_string();
    let n = len.as_i64()?.max(0) as usize;
    Ok(Value::String(s.chars().take(n).collect()))
}

/// RIGHT(str, len) - Return rightmost characters
pub fn right(value: &Value, len: &Value) -> Result<Value> {
    let s = value.to_string();
    let n = len.as_i64()?.max(0) as usize;
    let chars: Vec<char> = s.chars().collect();
    let start = chars.len().saturating_sub(n);
    Ok(Value::String(chars[start..].iter().collect()))
}

/// REPLACE(str, from, to) - Replace all occurrences
pub fn replace(value: &Value, from: &Value, to: &Value) -> Result<Value> {
    let s = value.to_string();
    let from_str = from.to_string();
    let to_str = to.to_string();
    Ok(Value::String(s.replace(&from_str, &to_str)))
}

/// REVERSE(str) - Reverse string
pub fn reverse(value: &Value) -> Result<Value> {
    Ok(Value::String(value.to_string().chars().rev().collect()))
}

/// LPAD(str, len, pad) - Pad left side
pub fn lpad(value: &Value, len: &Value, pad: &Value) -> Result<Value> {
    let s = value.to_string();
    let target_len = len.as_i64()? as usize;
    let pad_str = pad.to_string();

    if s.len() >= target_len {
        return Ok(Value::String(s.chars().take(target_len).collect()));
    }

    let padding_needed = target_len - s.len();
    let pad_repeated = pad_str.repeat((padding_needed / pad_str.len()) + 1);
    let result = format!("{}{}", &pad_repeated[..padding_needed], s);

    Ok(Value::String(result))
}

/// RPAD(str, len, pad) - Pad right side
pub fn rpad(value: &Value, len: &Value, pad: &Value) -> Result<Value> {
    let s = value.to_string();
    let target_len = len.as_i64()? as usize;
    let pad_str = pad.to_string();

    if s.len() >= target_len {
        return Ok(Value::String(s.chars().take(target_len).collect()));
    }

    let padding_needed = target_len - s.len();
    let pad_repeated = pad_str.repeat((padding_needed / pad_str.len()) + 1);
    let result = format!("{}{}", s, &pad_repeated[..padding_needed]);

    Ok(Value::String(result))
}

/// POSITION(substr IN str) / INSTR(str, substr)
pub fn position(substr: &Value, string: &Value) -> Result<Value> {
    let s = string.to_string();
    let sub = substr.to_string();

    match s.find(&sub) {
        Some(pos) => Ok(Value::Integer((pos + 1) as i64)), // SQL is 1-indexed
        None => Ok(Value::Integer(0)),
    }
}

/// REPEAT(str, count) - Repeat string n times
pub fn repeat(value: &Value, count: &Value) -> Result<Value> {
    let s = value.to_string();
    let n = count.as_i64()?.max(0) as usize;
    Ok(Value::String(s.repeat(n)))
}

/// SPACE(count) - Return string of spaces
pub fn space(count: &Value) -> Result<Value> {
    let n = count.as_i64()?.max(0) as usize;
    Ok(Value::String(" ".repeat(n)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concat() {
        let vals = vec![Value::from("Hello"), Value::from(" "), Value::from("World")];
        assert_eq!(concat(&vals).unwrap(), Value::from("Hello World"));
    }

    #[test]
    fn test_upper_lower() {
        assert_eq!(upper(&Value::from("hello")).unwrap(), Value::from("HELLO"));
        assert_eq!(lower(&Value::from("WORLD")).unwrap(), Value::from("world"));
    }

    #[test]
    fn test_substr() {
        let s = Value::from("Hello World");
        assert_eq!(
            substr(&s, &Value::Integer(1), Some(&Value::Integer(5))).unwrap(),
            Value::from("Hello")
        );
        assert_eq!(
            substr(&s, &Value::Integer(7), None).unwrap(),
            Value::from("World")
        );
    }

    #[test]
    fn test_trim() {
        assert_eq!(trim(&Value::from("  hello  ")).unwrap(), Value::from("hello"));
        assert_eq!(ltrim(&Value::from("  hello")).unwrap(), Value::from("hello"));
        assert_eq!(rtrim(&Value::from("hello  ")).unwrap(), Value::from("hello"));
    }

    #[test]
    fn test_replace() {
        assert_eq!(
            replace(&Value::from("hello world"), &Value::from("world"), &Value::from("rust")).unwrap(),
            Value::from("hello rust")
        );
    }
}
