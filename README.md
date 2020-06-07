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
