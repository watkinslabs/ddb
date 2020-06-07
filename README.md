# ddb
## A multi platform ETL tool for text files using natural SQL language

### ROADMAP Compatibility
+OS     +CLI+LIB+PERL+PYTHON 2+ Python 3 +Mono+NET+.NET Core+ Ansible + AWS Lambda +
+Linux  + x + x + x  + x      + x        +  x + x + x       + x       + x          +
+Windows+ x + x + x  + x      + x        +    + x + x       + x       +            +
+Mac    + x + x + x  + x      + x        +    +   +         + x       +            +
+Irix   + x + x + x  + x      + x        +    +   +         +         +            +
+-------+---+---+----+--------+----------+----+---+---------+---------+------------+



### SQL Language Support

- Select [DISTINCT] select_expr FROM [identifier]  [LEFT RIGHT, FULL OUTER,] JOIN idenftifier [as $alias] ON join_expr [WHERE where_expr] [GROUP BY group_expr] [LIMIT [$start,] $length]
- INSERT INTO $identifier (columns [,columns...]) VALUES (value[,value..,]])
- UPDATE  identifier SET identifier=value [,identifier=value] [WHERE where_expr]
- DELETE FROM  identifier [WHERE where_expr]
- TRUNCATE TABLE $identifier
- CREATE TABLE identifier (columns) file=$file delimiter=$char

### SQL FUNCTION
- date
- time
- datetime
- trim
- ltrim
- rtrim
- count ; aggregate func

