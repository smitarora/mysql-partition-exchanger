# mysql-partition-exchanger
A Python3 base utility to exchange partitions in MySQL based database

This utility will help you remove partitions in bulk depending on the conditions.

## Pre Requisites
To use this first make sure that all the requirements are satisfied. To install the requirements either install it via virtual environment or directly using pip 
> pip install -r requirements.txt

## Options

  --host HOST, -h HOST  The Hostname/IP ofdatabase

  --help                show this help message and exit

  --user USER, -u USER  The user to connect to database, if not specified it will look in ~/.my.cnf

  --password PASSWORD, -p PASSWORD
                        The Password to connect to database, if not specified it will look in ~/.my.cnf

  --port PORT, -P PORT  The Port to connect to database, if not specified it will look in ~/.my.cnf

  --table TABLE, -t TABLE
                        The table whose partitions should be exchanged

  --database DATABASE, -d DATABASE
                        The database or schema name

  --condition CONDITION, -c CONDITION
                        The condition that must be applied to check for partitions to be dropped. It will be checked in NFORMATION_SCHEMA.PARTITIONS

  --dry-run DRY_RUN, -dr DRY_RUN
                        Specifying this will not run modify or create any table. Only checks will be done and queries will be printed

  --log-file LOG_FILE, -l LOG_FILE
                        log file name

  --lock-wait-timout LOCK_WAIT_TIMOUT, -lt LOCK_WAIT_TIMOUT
                        Lock timeout. If not provided default value of 5 seconds is applied

  --exit-if-fail, -e    If enabled it will continue to exchange other partitions if it fails for one

  --retries RETRIES, -r RETRIES
                        no of retries before it should exit

  --exchange_name EXCHANGE_NAME, -en EXCHANGE_NAME
                        Suffix Name of intermediary table that should act as template for other tables. If not provide, default name source_table_template will be used

## Example

Consider the table salaries in employees database:

>CREATE TABLE `salaries` (
>
>  `emp_no` int NOT NULL,
>
>  `salary` int NOT NULL,
>
>  `from_date` date NOT NULL,
>
>  `to_date` date NOT NULL,
>
>  PRIMARY KEY (`emp_no`,`from_date`)
>
>) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
>
>/*!50500 PARTITION BY RANGE  COLUMNS(from_date)
>
>(    PARTITION p01 VALUES LESS THAN ('1985-12-31') ENGINE = InnoDB,
>    
>     PARTITION p02 VALUES LESS THAN ('1986-12-31') ENGINE = InnoDB,
>    
>     PARTITION p03 VALUES LESS THAN ('1987-12-31') ENGINE = InnoDB,
>    
>     PARTITION p04 VALUES LESS THAN ('1988-12-31') ENGINE = InnoDB,
>    
>     PARTITION p05 VALUES LESS THAN ('1989-12-31') ENGINE = InnoDB,
>    
>     PARTITION p06 VALUES LESS THAN ('1990-12-31') ENGINE = InnoDB,
>    
>     PARTITION p07 VALUES LESS THAN ('1991-12-31') ENGINE = InnoDB,
>    
>     PARTITION p08 VALUES LESS THAN ('1992-12-31') ENGINE = InnoDB,
>    
>     PARTITION p09 VALUES LESS THAN ('1993-12-31') ENGINE = InnoDB,
>    
>     PARTITION p10 VALUES LESS THAN ('1994-12-31') ENGINE = InnoDB,
>    
>     PARTITION p11 VALUES LESS THAN ('1995-12-31') ENGINE = InnoDB,
>    
>     PARTITION p12 VALUES LESS THAN ('1996-12-31') ENGINE = InnoDB,
>    
>     PARTITION p13 VALUES LESS THAN ('1997-12-31') ENGINE = InnoDB,
>    
>     PARTITION p14 VALUES LESS THAN ('1998-12-31') ENGINE = InnoDB,
>    
>     PARTITION p15 VALUES LESS THAN ('1999-12-31') ENGINE = InnoDB,
>    
>     PARTITION p16 VALUES LESS THAN ('2000-12-31') ENGINE = InnoDB,
>    
>     PARTITION p17 VALUES LESS THAN ('2001-12-31') ENGINE = InnoDB,
>    
>     PARTITION p18 VALUES LESS THAN ('2002-12-31') ENGINE = InnoDB,
>    
>     PARTITION p19 VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB) */

If you need to exchange partitions having data older than '1992-01-01', you can use following command.

> python3 partition_exchange.py  -d employees -c " STR_TO_DATE(SUBSTRING(PARTITION_DESCRIPTION,2,10), '%Y-%m-%d')<CONVERT('1992-01-01',DATE) AND PARTITION_DESCRIPTION<>'MAXVALUE'" -l "exchange_art.log" -t salaries -H localhost  -r 5 

It will create a table ```_salaries_template``` and create seven tables ``` _salaries_p01,_salaries_p02,_salaries_p03,_salaries_p04,_salaries_p05,_salaries_p06 and _salaries_p07 ``` and exchange partitions ``` p01,p02,p03,p04,p05,p06,p07 ``` with these tables.

It will not DELETE the tables ```_salaries_template, _salaries_p01,_salaries_p02,_salaries_p03,_salaries_p04,_salaries_p05,_salaries_p06 and _salaries_p07 ```
