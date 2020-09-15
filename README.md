# aws-lambda-python-kinesis-aurora

## Database(RDS)
- Create database
```
USE ug2database
```

- Create `users` table
```
CREATE TABLE users 
(swid varchar(50) NOT NULL,
 birth_dt varchar(20),
 gender_cd varchar(10))
DEFAULT CHARSET=utf8
ENGINE=InnoDB
```

- Create `products` table
```
CREATE TABLE products 
(url varchar(20) NOT NULL PRIMARY KEY,
 category varchar(32),
 id varchar(50))
DEFAULT CHARSET=utf8
ENGINE=InnoDB
```

- result
```
select count(*) from users
38455

select count(*) from products
16
```