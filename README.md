# ddb
## A multi platform ETL tool for text files using natural SQL language

This is a total rebuild of ddb, a python or cython module, in pure c. It's a WIP and not funcitonal.

### ROADMAP Compatibility
|OS     |CLI|LIB|PERL|PYTHON 2| Python 3 |Mono|NET|.NET Core| Ansible | AWS Lambda |
|-------|---|---|----|--------|----------|----|---|---------|---------|------------|
|Linux  | x | x | x  | x      | x        |  x | x | x       | x       | x          |
|Windows| x | x | x  | x      | x        |    | x | x       | x       |            |
|Mac    | x | x | x  | x      | x        |    |   |         | x       |            |
|Irix   | x | x | x  | x      | x        |    |   |         |         |            |



### SQL Language Support

- Select [DISTINCT] select_expr FROM [identifier]  [LEFT RIGHT, FULL OUTER,] JOIN idenftifier [as $alias] ON join_expr [WHERE where_expr] [GROUP BY group_expr] [LIMIT [$start,] $length]
- INSERT INTO $identifier (columns [,columns...]) VALUES (value[,value..,]])
- UPDATE  identifier SET identifier=value [,identifier=value] [WHERE where_expr]
- DELETE FROM  identifier [WHERE where_expr]
- TRUNCATE TABLE $identifier
- CREATE TABLE identifier (columns) file=$file delimiter=$char
- USE $database


### SQL FUNCTION
- date
- time
- datetime
- trim
- ltrim
- rtrim
- count ; aggregate func

### BASH Use
- pipe the statments directly into ddb
```
echo create table company.users (fname,lname,age) file="/var/company/users.csv" delimiter='|';select * from company.users where lname='sanders'|ddb
```
OR

- pipe the statments directly into ddb and use a premade definition file

```
# file ->company.ddl.sql
create table company.users (fname,lname,age) file="/var/company/users.csv" delimiter='|';


$ echo select * from company.users where lname='sanders'|ddb company.ddl.sql

```

### Python Use
```
import ddb
ddb.init(confid_dir=config_dir)
res=ddb.query("select * from company.users where lname='sanders'")
if res.success==True:
 print ("successfull")
 for row in res.data:
  for column in row:
   print (column)

```

### PERL Use
```
uh.. perl guys help?
```



```
# Debugging output
builds/ddbc -d test/simple.txt
row_count_max:1003003001 -- CREATE_TABLE DEBUG -------------
 - identifier: test.mock1
 - Column Type: STRING
 - Column Value: id
 - Column Type: STRING
 - Column Value: first
 - Column Type: STRING
 - Column Value: last
  file:          /home/nd/repos/wlddb/test/test.mock.csv 
  column:        , 
  strict:        1 
 --
 -- CREATE_TABLE DEBUG -------------
 - identifier: test.mock2
 - Column Type: STRING
 - Column Value: id
 - Column Type: STRING
 - Column Value: first
 - Column Type: STRING
 - Column Value: last
  file:          /home/nd/repos/wlddb/test/test.mock.csv 
  column:        , 
  strict:        1 
 --
 -- CREATE_TABLE DEBUG -------------
 - identifier: test.mock3
 - Column Type: STRING
 - Column Value: iid
 - Column Type: STRING
 - Column Value: first
 - Column Type: STRING
 - Column Value: last
  file:          /home/nd/repos/wlddb/test/test.mock.csv 
  column:        , 
  strict:        1 
 --
 # Use
 - database: test
 # Select
 ## HAS DISTINCT
 - identifier: t1.id
   - alias: id1
   - ordinal: 0
 - identifier: t2.id
   - alias: id2
   - ordinal: 1
 - identifier: t3.iid
   - alias: id3
   - ordinal: 2
 - identifier: t1.first
   - alias: first1
   - ordinal: 3
 - identifier: t2.first
   - alias: first2
   - ordinal: 4
 - identifier: t3.first
   - alias: first3
   - ordinal: 5
 ## FROM
 - identifier: test.mock1
   - alias: t1
 ## JOIN
  Type: LEFT_JOIN
test.mock1 ALIAS: t2
   mode:   1 ,  Identifier: t2.id
   mode:   2 , comparison :ASSIGNMENT   Litteral: [NUMERIC] '1'
  Type: RIGHT_JOIN
test.mock3 ALIAS: t3
   mode:   1 ,  Identifier: t3.iid
   mode:   2 , comparison :ASSIGNMENT   Litteral: [NUMERIC] '1'
 ---WHERE---
   mode:   2 ,  Litteral: [NUMERIC] '1'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '15'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '6'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '8'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '456'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '123123'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '4'
   mode:   2 , comparison :ASSIGNMENT   Litteral: [NUMERIC] '15'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '6'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '8'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '456'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '123123'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '4'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '11'
   mode:   6 , logical    :AND 
   mode:   2 ,  Litteral: [NUMERIC] '2'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '2'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '41'
   mode:   2 , comparison :ASSIGNMENT   Litteral: [NUMERIC] '4'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '1'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '5'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '35'
   mode:   6 , logical    :AND 
   mode:   2 ,  Litteral: [NUMERIC] '5'
   mode:   6 , logical    :AND 
   mode:   2 ,  Litteral: [NUMERIC] '4'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '1'
   mode:   6 , logical    :OR 
   mode:   2 ,  Litteral: [NUMERIC] '1'
   mode:   2 , comparison :ASSIGNMENT   Litteral: [NUMERIC] '1'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '2'
   mode:   6 , logical    :OR 
   mode:   2 ,  Litteral: [NUMERIC] '2'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '3'
   mode:   6 , logical    :AND 
   mode:   2 ,  Litteral: [NUMERIC] '5'
   mode:   2 , comparison :ASSIGNMENT  uinary     :MINUS   Litteral: [NUMERIC] '1'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '6'
   mode:   2 , arithmetic :MINUS   Litteral: [NUMERIC] '1'
   mode:   6 , logical    :OR 
   mode:   2 ,  Litteral: [REAL] '3.4'
   mode:   2 , arithmetic :PLUS   Litteral: [REAL] '4.1'
   mode:   2 , comparison :ASSIGNMENT   Litteral: [NUMERIC] '3'
   mode:   2 , arithmetic :PLUS   Litteral: [NUMERIC] '10'
   mode:   2 , arithmetic :MINUS   Litteral: [REAL] '0.5'
   mode:   2 , arithmetic :MINUS   Litteral: [NUMERIC] '5'
   mode:   6 , logical    :OR 
   mode:   2 ,  Litteral: [REAL] '5.2'

# Cursor
- Database: test
- Created : 1593615259.275536291
- Ended   : 1593615259.305501484
- Ellapsed: 0.029965193
- Status  : SUCCESS
 # dataset
 - column length: 6
 - row_length: 0
 - position: 0

```