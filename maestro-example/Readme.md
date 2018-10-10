# Maestro examples

The `maestro-example` package provides examples of using `maestro` for a number of ETL tasks.

## Prerequisites

All of the dependencies required to build and test `maestro` and `maestro-example` will be obtained
automatically by SBT. To run the examples outside of the test environment you will require
configured Hadoop, Hive, and MySQL (or other SQL database) environments. The test scripts were written
and tested on a variant of Cloudera CDH 5.3.0, which includes all required external dependencies preconfigured.

## Building

To build the examples, open a terminal in the project root and run:

```
$ ./sbt
> project example
> assembly
``` 

## Testing

To run the examples in a unit test environment, open a terminal in the project root and run:

```
$ ./sbt
> project example
> test
```

## Running

To run the examples outside of the test environment, first build the examples, ensuring that a JAR is
produced in the `maestro-example/target/scala-2.11` directory.

Note that these examples assume that Hadoop, Hive, and a MySQL server have been correctly configured
on the local machine. The `test-CustomerSqoopExportJob` and `test-CustomerSqoopImportJob` will need
to be modified to reflect the correct MySQL settings, such as to change the username and password
used to connect to MySQL. Other SQL databases can be substituted by modifying these scripts.

### CustomerAutomapJob

The `CustomerAutomapJob` takes customer data in a local file in pipe-separated-values format,
uploads it to HDFS in the same format, loads the data from HDFS into a Thrift format, transforms
the data into account data, then writes the account data to a Hive table, partitioned by date. For
full details see the source code.


To run the `CustomerAutomapJob`, open a terminal in the project root and run:

```
$ ./maestro-example/bin/test-CustomerAutomapJob
```

The input data can be found in `./maestro-example/src/test/resources/customer/dataFeed`.

To view the data stored in HDFS, run:

```
$ hdfs dfs -ls maestro_example_CustomerAutomapJob/source/customer/customer/customer/2014/08/23
$ hdfs dfs -cat maestro_example_CustomerAutomapJob/source/customer/customer/customer/2014/08/23/customer_20140823.txt
```

To view the data stored in Hive, run:

```
$ hive --database maestro_example_CustomerAutomapJob_customer_customer
hive> SHOW TABLES;
hive> SELECT * FROM account;
```

### CustomerHiveJob

The `CustomerHiveJob` takes customer data in a local file in pipe-separated-values format, uploads
it to HDFS in the same format, loads the data from HDFS into a Thrift format, then writes the
customer data to Hive tables, one partitioned by date, and another by category.

To run the `CustomerHiveJob`, open a terminal in the project root and run:

```
$ ./maestro-example/bin/test-CustomerHiveJob
```

The input data can be found in `./maestro-example/src/test/resources/customer/dataFeed`.

To view the data stored in HDFS, run:

```
$ hdfs dfs -ls maestro_example_CustomerHiveJob/source/customer/customer/customer/2014/08/23
$ hdfs dfs -cat maestro_example_CustomerHiveJob/source/customer/customer/customer/2014/08/23/customer_20140823.txt
```

To view the data stored in Hive, run:

```
$ hive --database maestro_example_CustomerHiveJob_customer_customer
hive> SHOW TABLES;
hive> SELECT * FROM by_cat;
hive> SELECT * FROM by_date;
```


### CustomerSqoopExportJob

The `CustomerSqoopExportJob` takes customer data in a local file in pipe-separated-values format,
uploads it to HDFS in the same format, loads the data from HDFS into a Thrift format, then writes
the customer data to an SQL database.

To run the `CustomerSqoopExportJob`, open a terminal in the project root and run:

```
$ ./maestro-example/bin/test-CustomerSqoopExportJob
```

The input data can be found in `./maestro-example/src/test/resources/sqoop-customer/dataFeed`.

To view the data stored in HDFS, run:

```
$ hdfs dfs -ls maestro_example_CustomerSqoopExportJob/source/customer/customer/customer/2014/08/23
$ hdfs dfs -cat maestro_example_CustomerSqoopExportJob/source/customer/customer/customer/2014/08/23/customer_20140823.txt
```

To view the data stored in MySQL, run:

```
$ mysql maestro_example_CustomerSqoopExportJob
mysql> SHOW TABLES;
mysql> SELECT * FROM customer_export;
```

### CustomerSqoopImportJob

The `CustomerSqoopImportJob` takes customer data in an SQL database, imports the data into HDFS in a
text format, loads the data from HDFS into a Thrift format, then writes the customer data to a Hive
table, partitioned by category.

To run the `CustomerSqoopImportJob`, open a terminal in the project root and run:

```
$ ./maestro-example/bin/test-CustomerSqoopImportJob
```

The input data can be found in `./maestro-example/src/test/resources/sqoop-customer/dataFeed`.

To view the data stored in MySQL, run:

```
$ mysql maestro_example_CustomerSqoopImportJob
mysql> SHOW TABLES;
mysql> SELECT * FROM customer_import;
```

To view the data stored in HDFS, run:

```
$ hdfs dfs -ls maestro_example_CustomerSqoopImportJob/source/sales/books/customer_import/2015/07/06
$ hdfs dfs -cat maestro_example_CustomerSqoopImportJob/source/sales/books/customer_import/2015/07/06/part-m-00000
$ hdfs dfs -cat maestro_example_CustomerSqoopImportJob/source/sales/books/customer_import/2015/07/06/part-m-00001
```

To view the data stored in Hive, run:

```
$ hive --database maestro_example_customersqoopimportexecutionjob_sales_books
hive> SHOW TABLES;
hive> SELECT * FROM by_cat;
```
