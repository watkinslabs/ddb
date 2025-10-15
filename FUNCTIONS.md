# DDB Function Reference

Comprehensive list of all SQL functions available in DDB-Rust.

## Categories

- [Math Functions](#math-functions)
- [String Functions](#string-functions)
- [Type Conversion](#type-conversion)
- [Date/Time Functions](#datetime-functions)
- [Aggregate Functions](#aggregate-functions)
- [System Functions](#system-functions)
- [Conditional Functions](#conditional-functions)
- [Utility Functions](#utility-functions)

---

## Math Functions

### ABS(x)
Returns the absolute value of x.
```sql
SELECT ABS(-5) → 5
SELECT ABS(3.14) → 3.14
```

### CEIL(x) / CEILING(x)
Rounds x up to the nearest integer.
```sql
SELECT CEIL(3.2) → 4
SELECT CEIL(-3.8) → -3
```

### FLOOR(x)
Rounds x down to the nearest integer.
```sql
SELECT FLOOR(3.8) → 3
SELECT FLOOR(-3.2) → -4
```

### ROUND(x, [decimals])
Rounds x to the specified number of decimal places.
```sql
SELECT ROUND(3.456) → 3
SELECT ROUND(3.456, 2) → 3.46
```

### SQRT(x)
Returns the square root of x.
```sql
SELECT SQRT(16) → 4.0
SELECT SQRT(2) → 1.414...
```

### POW(x, y) / POWER(x, y)
Returns x raised to the power of y.
```sql
SELECT POW(2, 3) → 8.0
SELECT POW(10, 2) → 100.0
```

### EXP(x)
Returns e^x.
```sql
SELECT EXP(1) → 2.718...
```

### LN(x) / LOG(x)
Returns the natural logarithm of x.
```sql
SELECT LN(2.718) → 1.0
```

### LOG10(x)
Returns the base-10 logarithm of x.
```sql
SELECT LOG10(100) → 2.0
```

### MOD(x, y)
Returns x modulo y.
```sql
SELECT MOD(10, 3) → 1
SELECT MOD(17, 5) → 2
```

### SIGN(x)
Returns -1, 0, or 1 depending on the sign of x.
```sql
SELECT SIGN(-5) → -1
SELECT SIGN(0) → 0
SELECT SIGN(10) → 1
```

### TRUNC(x, [decimals])
Truncates x to the specified decimal places.
```sql
SELECT TRUNC(3.456) → 3
SELECT TRUNC(3.456, 2) → 3.45
```

### PI()
Returns the value of π.
```sql
SELECT PI() → 3.141592...
```

### RAND() / RANDOM()
Returns a random number between 0 and 1.
```sql
SELECT RAND() → 0.234... (random)
```

---

## String Functions

### CONCAT(str1, str2, ...)
Concatenates strings.
```sql
SELECT CONCAT('Hello', ' ', 'World') → 'Hello World'
```

### CONCAT_WS(separator, str1, str2, ...)
Concatenates strings with a separator.
```sql
SELECT CONCAT_WS(',', 'a', 'b', 'c') → 'a,b,c'
```

### UPPER(str) / UCASE(str)
Converts string to uppercase.
```sql
SELECT UPPER('hello') → 'HELLO'
```

### LOWER(str) / LCASE(str)
Converts string to lowercase.
```sql
SELECT LOWER('WORLD') → 'world'
```

### LENGTH(str) / CHAR_LENGTH(str)
Returns the length of a string.
```sql
SELECT LENGTH('Hello') → 5
```

### TRIM(str)
Removes leading and trailing whitespace.
```sql
SELECT TRIM('  hello  ') → 'hello'
```

### LTRIM(str)
Removes leading whitespace.
```sql
SELECT LTRIM('  hello') → 'hello'
```

### RTRIM(str)
Removes trailing whitespace.
```sql
SELECT RTRIM('hello  ') → 'hello'
```

### SUBSTR(str, pos, [len]) / SUBSTRING(str, pos, [len])
Extracts a substring.
```sql
SELECT SUBSTR('Hello World', 1, 5) → 'Hello'
SELECT SUBSTR('Hello World', 7) → 'World'
```

### LEFT(str, len)
Returns the leftmost len characters.
```sql
SELECT LEFT('Hello', 3) → 'Hel'
```

### RIGHT(str, len)
Returns the rightmost len characters.
```sql
SELECT RIGHT('Hello', 3) → 'llo'
```

### REPLACE(str, from, to)
Replaces all occurrences of from with to.
```sql
SELECT REPLACE('Hello World', 'World', 'Rust') → 'Hello Rust'
```

### REVERSE(str)
Reverses a string.
```sql
SELECT REVERSE('Hello') → 'olleH'
```

### LPAD(str, len, pad)
Pads string on the left.
```sql
SELECT LPAD('5', 3, '0') → '005'
```

### RPAD(str, len, pad)
Pads string on the right.
```sql
SELECT RPAD('5', 3, '0') → '500'
```

### POSITION(substr IN str) / INSTR(str, substr)
Returns position of substring (1-indexed, 0 if not found).
```sql
SELECT POSITION('World' IN 'Hello World') → 7
```

### REPEAT(str, count)
Repeats string n times.
```sql
SELECT REPEAT('Ha', 3) → 'HaHaHa'
```

### SPACE(count)
Returns a string of spaces.
```sql
SELECT SPACE(5) → '     '
```

---

## Type Conversion

### CAST(value AS type)
SQL standard type conversion.
```sql
SELECT CAST('123' AS INTEGER) → 123
SELECT CAST(42 AS STRING) → '42'
```

Supported types: INTEGER, FLOAT, STRING, BOOLEAN, DATE, DATETIME

### CONVERT(value, type)
MySQL-style conversion (same as CAST).
```sql
SELECT CONVERT('3.14', FLOAT) → 3.14
```

### ATOF(str)
Converts string to float.
```sql
SELECT ATOF('3.14') → 3.14
```

### ATOI(str)
Converts string to integer.
```sql
SELECT ATOI('42') → 42
```

### TO_STRING(value)
Converts any value to string.
```sql
SELECT TO_STRING(123) → '123'
```

### TO_NUMBER(str)
Converts string to number (int or float).
```sql
SELECT TO_NUMBER('123') → 123
SELECT TO_NUMBER('12.5') → 12.5
```

### HEX(n)
Converts number to hexadecimal.
```sql
SELECT HEX(255) → 'FF'
```

### BIN(n)
Converts number to binary.
```sql
SELECT BIN(7) → '111'
```

### OCT(n)
Converts number to octal.
```sql
SELECT OCT(8) → '10'
```

### FORMAT(number, decimals)
Formats number with thousands separator.
```sql
SELECT FORMAT(1234567.89, 2) → '1,234,567.89'
```

---

## Date/Time Functions

### NOW() / CURRENT_TIMESTAMP()
Returns current date and time.
```sql
SELECT NOW() → '2024-03-15 14:30:00'
```

### CURDATE() / CURRENT_DATE()
Returns current date.
```sql
SELECT CURDATE() → '2024-03-15'
```

### CURTIME() / CURRENT_TIME()
Returns current time.
```sql
SELECT CURTIME() → '14:30:00'
```

### DATE(datetime)
Extracts date part.
```sql
SELECT DATE('2024-03-15 14:30:00') → '2024-03-15'
```

### TIME(datetime)
Extracts time part.
```sql
SELECT TIME('2024-03-15 14:30:00') → '14:30:00'
```

### YEAR(date)
Extracts year.
```sql
SELECT YEAR('2024-03-15') → 2024
```

### MONTH(date)
Extracts month (1-12).
```sql
SELECT MONTH('2024-03-15') → 3
```

### DAY(date)
Extracts day of month (1-31).
```sql
SELECT DAY('2024-03-15') → 15
```

### HOUR(time)
Extracts hour (0-23).
```sql
SELECT HOUR('14:30:00') → 14
```

### MINUTE(time)
Extracts minute (0-59).
```sql
SELECT MINUTE('14:30:00') → 30
```

### SECOND(time)
Extracts second (0-59).
```sql
SELECT SECOND('14:30:45') → 45
```

### DAYOFWEEK(date)
Day of week (1=Sunday, 7=Saturday).
```sql
SELECT DAYOFWEEK('2024-03-15') → 6
```

### DAYNAME(date)
Name of day.
```sql
SELECT DAYNAME('2024-03-15') → 'Friday'
```

### MONTHNAME(date)
Name of month.
```sql
SELECT MONTHNAME('2024-03-15') → 'March'
```

### DAYOFYEAR(date)
Day of year (1-366).
```sql
SELECT DAYOFYEAR('2024-03-15') → 75
```

### WEEK(date)
Week number of year.
```sql
SELECT WEEK('2024-03-15') → 11
```

### QUARTER(date)
Quarter of year (1-4).
```sql
SELECT QUARTER('2024-03-15') → 1
```

### DATEDIFF(date1, date2)
**Days between two dates.**
```sql
SELECT DATEDIFF('2024-03-20', '2024-03-15') → 5
```

### DATEADD(date, interval, unit)
**Add interval to date.**
```sql
SELECT DATEADD('2024-03-15', 5, 'day') → '2024-03-20'
SELECT DATEADD('2024-03-15', 2, 'month') → '2024-05-15'
SELECT DATEADD('2024-03-15', 1, 'year') → '2025-03-15'
```

### DATESUB(date, interval, unit)
**Subtract interval from date.**
```sql
SELECT DATESUB('2024-03-15', 5, 'day') → '2024-03-10'
```

### TIMESTAMPDIFF(unit, datetime1, datetime2)
**Difference between datetimes in specified unit.**
```sql
SELECT TIMESTAMPDIFF('day', '2024-03-10', '2024-03-15') → 5
SELECT TIMESTAMPDIFF('hour', '2024-03-15 10:00', '2024-03-15 14:00') → 4
```

### AGE(date1, [date2])
**Calculate age (years between dates).**
```sql
SELECT AGE('1990-01-01', '2024-03-15') → 34
SELECT AGE('1990-01-01') → (age from date to now)
```

### DATE_FORMAT(date, format)
Format date/time.
```sql
SELECT DATE_FORMAT(NOW(), '%Y-%m-%d') → '2024-03-15'
```

### UNIX_TIMESTAMP([date])
Seconds since Unix epoch.
```sql
SELECT UNIX_TIMESTAMP('2024-01-01') → 1704067200
```

### FROM_UNIXTIME(timestamp)
Convert Unix timestamp to datetime.
```sql
SELECT FROM_UNIXTIME(1704067200) → '2024-01-01 00:00:00'
```

---

## Aggregate Functions

### COUNT(*)
Count all rows.
```sql
SELECT COUNT(*) FROM users
```

### COUNT(expr)
Count non-null values.
```sql
SELECT COUNT(email) FROM users
```

### SUM(expr)
Sum of values.
```sql
SELECT SUM(amount) FROM transactions
```

### AVG(expr)
Average of values.
```sql
SELECT AVG(price) FROM products
```

### MIN(expr)
Minimum value.
```sql
SELECT MIN(age) FROM users
```

### MAX(expr)
Maximum value.
```sql
SELECT MAX(salary) FROM employees
```

### GROUP_CONCAT(expr, [separator])
Concatenate values from group.
```sql
SELECT GROUP_CONCAT(name, ', ') FROM users
```

### STDDEV_POP(expr)
Standard deviation (population).
```sql
SELECT STDDEV_POP(score) FROM grades
```

### VAR_POP(expr)
Variance (population).
```sql
SELECT VAR_POP(value) FROM measurements
```

---

## System Functions

### DATABASE([name])
Returns current database name.
```sql
SELECT DATABASE() → 'main'
```

### VERSION()
Returns DDB version.
```sql
SELECT VERSION() → '0.1.0'
```

### UUID()
Generates a UUID.
```sql
SELECT UUID() → '550e8400-e29b-41d4-a716-446655440000'
```

### ROW_NUMBER()
Returns current row number.
```sql
SELECT ROW_NUMBER(), * FROM users
```

### USER()
Returns current user.
```sql
SELECT USER() → 'file_user'
```

### CONNECTION_ID()
Returns connection ID (process ID).
```sql
SELECT CONNECTION_ID() → 12345
```

---

## Conditional Functions

### IF(condition, true_value, false_value)
Conditional expression.
```sql
SELECT IF(age >= 18, 'Adult', 'Minor') FROM users
```

### IFNULL(expr, alt_value)
Returns alt_value if expr is NULL.
```sql
SELECT IFNULL(phone, 'N/A') FROM users
```

### NULLIF(expr1, expr2)
Returns NULL if expr1 == expr2.
```sql
SELECT NULLIF(value, 0) → (NULL if value is 0)
```

### COALESCE(val1, val2, ...)
Returns first non-NULL value.
```sql
SELECT COALESCE(phone, email, 'No contact') FROM users
```

### GREATEST(val1, val2, ...)
Returns greatest value.
```sql
SELECT GREATEST(10, 25, 15) → 25
```

### LEAST(val1, val2, ...)
Returns smallest value.
```sql
SELECT LEAST(10, 25, 15) → 10
```

### ISNULL(expr)
Check if value is NULL.
```sql
SELECT ISNULL(email) FROM users
```

---

## Utility Functions

### HASH(value)
Simple hash value.
```sql
SELECT HASH('test') → 12345678901234567890
```

### MD5(str)
MD5 hash (simplified implementation).
```sql
SELECT MD5('password')
```

### SHA1(str)
SHA1 hash.
```sql
SELECT SHA1('data')
```

### SHA256(str)
SHA256 hash.
```sql
SELECT SHA256('data')
```

### BASE64_ENCODE(str)
Encode to base64.
```sql
SELECT BASE64_ENCODE('Hello') → 'SGVsbG8='
```

### BASE64_DECODE(str)
Decode from base64.
```sql
SELECT BASE64_DECODE('SGVsbG8=') → 'Hello'
```

### URL_ENCODE(str)
URL encode string.
```sql
SELECT URL_ENCODE('hello world') → 'hello+world'
```

### URL_DECODE(str)
URL decode string.
```sql
SELECT URL_DECODE('hello+world') → 'hello world'
```

### SPLIT_PART(string, delimiter, field_num)
Split string and return nth part.
```sql
SELECT SPLIT_PART('a|b|c', '|', 2) → 'b'
```

### REGEXP_REPLACE(string, pattern, replacement)
Regex replace.
```sql
SELECT REGEXP_REPLACE('test123', '\d+', 'X') → 'testX'
```

### REGEXP_MATCH(string, pattern)
Test if string matches regex.
```sql
SELECT REGEXP_MATCH('test123', '\d+') → true
```

### LEVENSHTEIN(str1, str2)
Levenshtein distance between strings.
```sql
SELECT LEVENSHTEIN('kitten', 'sitting') → 3
```

---

## Usage Examples

### Data Cleaning
```sql
-- Normalize phone numbers
SELECT REGEXP_REPLACE(phone, '[^0-9]', '') FROM contacts

-- Title case names
SELECT CONCAT(UPPER(LEFT(first_name, 1)), LOWER(SUBSTR(first_name, 2))) FROM users
```

### Date Operations
```sql
-- Find records from last 7 days
SELECT * FROM logs WHERE DATE(timestamp) >= DATESUB(CURDATE(), 7, 'day')

-- Calculate age
SELECT name, AGE(birthdate) as age FROM users
```

### Data Analysis
```sql
-- Statistical summary
SELECT
  COUNT(*) as count,
  AVG(value) as mean,
  STDDEV_POP(value) as stddev,
  MIN(value) as min,
  MAX(value) as max
FROM measurements
```

### String Manipulation
```sql
-- Parse CSV field
SELECT SPLIT_PART(data, ',', 1) as col1,
       SPLIT_PART(data, ',', 2) as col2
FROM raw_data
```

---

## Function Categories Summary

| Category | Count | Examples |
|----------|-------|----------|
| Math | 13 | ABS, SQRT, POW, MOD |
| String | 18 | CONCAT, UPPER, SUBSTR, TRIM |
| Conversion | 9 | CAST, ATOF, HEX, FORMAT |
| Date/Time | 27 | NOW, DATEDIFF, DATEADD, YEAR |
| Aggregate | 9 | COUNT, SUM, AVG, MIN, MAX |
| System | 7 | DATABASE, VERSION, UUID |
| Conditional | 7 | IF, IFNULL, COALESCE |
| Utility | 11 | HASH, BASE64, REGEXP, SPLIT_PART |

**Total: 101 functions**

---

## Performance Notes

- All functions are implemented in native Rust for maximum performance
- String functions use zero-copy operations where possible
- Aggregate functions process data in streaming fashion
- Date/Time operations use `chrono` for accurate calculations
- Regular expressions are compiled once and cached

## Future Additions

Planned functions for future releases:
- JSON functions (JSON_EXTRACT, JSON_OBJECT)
- Array functions (ARRAY_AGG, UNNEST)
- Window functions (LAG, LEAD, RANK)
- More statistical functions (MEDIAN, PERCENTILE)
- Cryptographic functions (proper MD5/SHA implementations)
