# SQL and Amazon Wrappers

This repository holds two Python libraries. The first is for reading and writing to MySQL tables. The second is for retrieving daily orders for different marketplaces from Amazon and saving them. 

**Date:** Fall 2020 - Spring 2021

## Table of Contents

Here is an ordered outline of this file's contents:

* [Prerequisites](#prerequisites)
  * [Python Version](#python-version)
  * [Package Dependencies](#package-dependencies)
* [Using the Libraries](#using-the-libraries)
  * [Locally](#locally)
  * [Online](#online)
  * [Documentation Files](#documentation-files)
* [sql_lib](#sql_lib)
  * [Introduction to MySQL_Table_Schema](#introduction-to-mysql_table_schema)
  * [Introduction to MySQL_DB_Connection](#introduction-to-mysql_db_connection)
  * [Checking Schema Instances](#checking-schema-instances)
  * [Adding Data to a Schema](#adding-data-to-a-schema)
  * [Reading Data from a Schema](#reading-data-from-a-schema)
  * [Other Functions](#other-functions)
* [amazon_sp_lib](#amazon_sp_lib)
  * [SP_Orders_Retrieval](#sp_orders_retrieval)
* [Future Development](#future-development)
* [Built With](#built-with)
* [Authors](#authors)
* [Acknowledgments](#acknowledgments)

## Prerequisites

Before using these libraries, you will need to install a necessary Python version and package dependencies. This applies to using the libraries locally or online which is discussed in the following section. 

### Python Version

`sql_lib.py` has been tested on versions `3.6.9` to `3.9`. To determine if other versions are appropriate, you may need to visit the information on the dependencies for `sql_lib.py` listed below. 

`amazon_sp_lib.py` is built on the Amazon SP Python API and has been tested on Python version `3.9.1`. For more information, please visit the PyPI site [here](https://pypi.org/project/python-amazon-sp-api/).

### Package Dependencies

To use `sql_lib.py`, you will need to install `pymysql` and `pandas` using a package manager such as `pip`. 

To use `amazon_sp_lib.py`, you will need to install `pandas`, `tqdm`, and `python-amazon-sp-api` using a package manager such as `pip`.

In order to use some of these packages, you may need to install additional dependencies such as `chardet` and `numpy` which can also be done through `pip`.

## Using the Libraries

The libraries can be found in the `src/` folder inside the repository. 

### Locally

If you are using the libraries locally in Visual Studio Code or Jupyter Notebook you can clone or download the repository to your computer. To use them in the directory you downloaded them to, you can import them as normal with: 

```
import wrapper_classes.src.sql_lib
import wrapper_classes.src.amazon_sp_lib
```

### Online

To use them online on Google Colaboratory, you can clone them by creating a code cell and running: 

```
!git clone https://github.com/ChamiLamelas-ph/python_wrapper_classes.git
```

This will create a folder called `python_wrapper_classes` in the `Files` tab. However, Python will not search here for modules that can be imported. To add this to the path, run the following in a code cell: 

```
import sys
sys.path.insert(1,'python_wrapper_classes/src/')
```

The modules can now be interpreted directly as they are on the system path: 

```
import amazon_sp_lib
import sql_lib
```

### Documentation Files

In the following sections, you will see how to use the classes in these libraries on a basic level. For more information on parameters, errors, and returned values you can view the documentation for these two libraries found in the `docs` folder. 

To do this, download or clone the repository. Then, open `sql_lib.html` and `amazon_sp_lib.html` in a browser. These files are generated using the `pdoc3` documentation package and follow the [numpy documentation standard](https://numpydoc.readthedocs.io/en/latest/format.html#documenting-modules). A Windows generation script is given in `docgen.bat`.

## sql_lib

In this section, you will see how to basic operations with the two classes in `src/sql_lib.py`. For more detailed information such as parameters, errors, and return information read the documentation in `docs/sql_lib.html`.

### Introduction to MySQL_Table_Schema

The first class in this library is `MySQL_Table_Schema` which is used to represent a schema on a MySQL database. The following code creates a simple schema: 

```
students_table = MySQL_Table_Schema(
  'students',
  [
    MySQL_Table_Column('id','int(11)',False,'auto_increment'),
    MySQL_Table_Column('first_name','varchar(40)'),
    MySQL_Table_Column('last_name','varchar(40)'),
    MySQL_Table_Column('gpa','decimal(10,2)'),
    MySQL_Table_Column('graduation_year','int(4)')
  ],
  'id'
)
```

This code will create the `students` schema that has the columns `id`,`first_name`,`last-name`,`gpa`, and `graduation_year`. 

In order to specify these columns, you will also have to specify data types for each column. You will need to specify the *exact* data type (such as `int(11)` instead of `int` for `id`). 

Also, you can pass in additional information such as not allowing a column to be `NULL` as in the case of `id`. This is done via the `null_allowed` parameter of `MySQL_Table_Column` which is `True` by default. All primary key columns will automatically not be allowed to have `NULL` values. Therefore, the query above could have been simplified to: 

```
students_table = MySQL_Table_Schema(
    "students",
    [
        MySQL_Table_Column("id","int(11)",extra="auto_increment"),
        MySQL_Table_Column("first_name","varchar(40)"),
        MySQL_Table_Column("last_name","varchar(40)"),
        MySQL_Table_Column("gpa","decimal(10,2)"),
        MySQL_Table_Column("graduation_year","int(4)")
    ],
    "id"
)
```

Columns can also have extra attributes specified as a string such as `auto_increment` in the case of `id`.

Primary keys can be specified via the `primary_key` parameter in `MySQL_Table_Column`. For this table, the student `id` serves as the primary key. 

You can also specify constraints on the table via an additional pair of lists. For more information on constraints, view the constructor documentation in `docs/sql_lib.html`.

### Introduction to MySQL_DB_Connection

The second class in the library in this library is `MySQL_DB_Connection` which is used to represent a connection to a MySQL database and can be used to perform database read and write operations. The following code creates a fake connection: 

```
conn = MySQL_DB_Connection(
  host='host.com',
    user='user', 
    password='pass', 
    port=1000, 
    database='database'
)
```

To establish an actual connection, you will need to retrieve these credentials from your database. 

### Checking Schema Instances

With the combination of these two classes, you can perform a variety of operations. For instance, you can create an instance of `students` on the fake database above:

```
students_table.create_on_db(conn)
```

You can confirm that `students` is on the database:

```
students_table.check_on_db(conn)
```

### Adding Data to a Schema

If you want to add some students' information to `students`, first prepare the information in a pandas `DataFrame`:

```
data = pd.DataFrame([
  ['john','smith',3.80,2010],
  ['james','lincoln',4.00,2012],
  ['bill','thomas',3.10,2011]
], columns=students_columns)
```

Note that the columns of `data` must match the columns of `schema_cols` parameter of the `insert()` function. In this table, the `id` column is automatically updated, so we will only be inserting the other `4` columns. That is why `students_columns` is defined for the creation of the `DataFrame` and the insertion below:

```
students_columns = students_table.get_column_names()[1:]
```

`get_column_names()` returns a list of the names of the columns in the table you are updating. You can now upload the data into `students_table` using `conn`:

```
conn.insert(students_table,data,students_columns)
```

If you want to upload only one row quickly, you can pass in a `list`. Again, you need to use `students_columns`.

```
conn.insert(students_table,['connor','smith',3.24,2010],students_columns)
```

The entries of the list should be ordered according to the columns of the table `students_table.columns`. 

While the syntax is easier, do not upload many rows using this strategy. If you know you will be doing many uploads, build a `DataFrame` first and pass it in as shown above. This will be more efficient.

**Warning:** When you are inserting only a subset of the columns or have a primary key that has `auto_increment`, it will lead to duplicate data even if the `overwrite` parameter is set to `False`. This is because `overwrite` simply chooses whether to `INSERT` or `REPLACE` and MySQL will determine whether there is a primary key overwrite or not.

### Reading Data from a Schema

You can read data from `students` as follows: 

```
conn.read(
  students_table,
  'graduation_year = 2008'
)
```

This will return a `DataFrame` with up to `5` rows of students who graduated in `2008`. If you want to make sure that you get all rows matching this `where` clause, you need to specify:

```
conn.read(
  students_table,
  'graduation_year = 2008',
  limit=None
)
```

Requiring the `limit` parameter serves as a safety check. That way, you will be aware that you will be downloading all of the data into a `DataFrame` in memory. 

If you wanted to select just the GPA in this query, do the following: 

```
conn.read(
  students_table,
  ['gpa'],
  'graduation_year = 2008',
  limit=None
)
```

By default, all columns will be returned.

**WARNING:** the information in the `where` parameter of `read()` is run as SQL without any checking. Thus, it is vulnerable to a SQL injection if used on unfiltered user input. Therefore, it is best to restrict user inputs and then dynamically build a safe query when using this function.

### Other Functions

You are able to perform other operations in both of these classes. For instance, you can delete rows from a schema, retrieve values for particular primary keys, and run your own queries. To see how to do this and read more about the operations discussed previously, read `docs/sql_lib.html`. Once you have put a schema on a database and want to use it in future (and you know its name), you can quickly create a `MySQL_Table_Schema` instance from the schema stored on a `MySQL_DB_Connection` using the `get_schema` method.

## amazon_sp_lib

This file has the `SP_Orders_Retrieval` class which uses the Python sp API to retrieve orders information for a particular day and country. This class is far easier to use than `AmazonOrderRetrieval` which will most likely be deprecated as Amazon plans to phase out the mws API in favor of the sp API.

### SP_Orders_Retrieval

The first step in retrieving orders is to initialize two dictionaries with credentials for North America and Europe as follows

```
NA_creds = dict(
    refresh_token='your North America refresh token',
    lwa_client_secret='your North America lwa client secret',
    aws_access_key='your North America aws access key',
    aws_secret_key='your North America aws secret key',
    role_arn='your North America role arn',
)

EUR_creds = dict(
    refresh_token='your Europe refresh token',
    lwa_client_secret='your Europe lwa client secret',
    aws_access_key='your Europe aws access key',
    aws_secret_key='your Europe aws secret key',
    role_arn='your Europe role arn',
)
```

`NA_creds` will be used to retrieve orders for all marketplaces in North America specified in `SP_Orders_Retrieval.NA_CODES`. `EUR_creds` will be used to retrieve orders for all marketplaces in Europe specified in `SP_Orders_Retrieval.EUR_CODES`. 

You will also need to build a dictionary `rates` that maps a (day,currency) -> float. Currencies should be Amazon supported currencies. This will be used to convert prices to USD. Then, you can initialize a `SP_Orders_Retrieval` object:

```
obj = SP_Orders_Retrieval(NA_creds, EUR_creds, rates)
```

When constructing an `SP_Orders_Retrieval` object you are able to specify a keyword argument `timezone`. By default, it is `None` and the orders for a day retrieved in `retrieve_orders` (see below) will be retrieved via a timzeone corresponding to the provided country code as indicated in the dictionary `SP_Orders_Retrieval.REPORT_TIMEZONES`. However, if you want all your orders to go through one timezone, you can specify `timezone` to be a valid `pytz` timezone which will then cause `SP_Orders_Retrieval.REPORT_TIMEZONES` to be ignored. A list of valid `pytz` timezones can be found [here](https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568). Note however that columns of `datetime` in `orders_df` and `items_df` (see below) are all UTC/GMT time as that is the way in which they are returned from Amazon.

The timezones in the dictionary are selected based on [this FAQ](https://developer.amazon.com/docs/reports-promo/reporting-FAQ.html) For those that are not listed, the closest timezone from FAQ is selected.

Once an `SP_Orders_Retrieval` object has been constructed, you can retrieve orders for a particular marketplace specified by a country code and date specified by a yyyy-MM-dd string. 

```
orders_df, items_df = obj.retrieve_orders('US','2021-02-01')
```

`orders_df` is a Pandas DataFrame with the following columns and types

```
Data columns (total 18 columns):
 #   Column                           Non-Null Count  Dtype         
---  ------                           --------------  -----         
 0   amazon_order_id                  12 non-null     object        
 1   purchase_date                    12 non-null     datetime64[ns]
 2   last_update_date                 12 non-null     datetime64[ns]
 3   order_status                     12 non-null     object        
 4   fulfillment_channel              12 non-null     object        
 5   sales_channel                    12 non-null     object        
 6   ship_service_level               12 non-null     object        
 7   order_total_usd                  12 non-null     float64       
 8   number_of_items_shipped          12 non-null     int64         
 9   number_of_items_unshipped        12 non-null     int64         
 10  is_replacement_order             12 non-null     bool          
 11  marketplace_id                   12 non-null     object        
 12  shipment_service_level_category  12 non-null     object        
 13  earliest_ship_date               12 non-null     datetime64[ns]
 14  latest_ship_date                 12 non-null     datetime64[ns]
 15  is_prime                         12 non-null     bool          
 16  is_global_express_enabled        12 non-null     bool          
 17  is_premium_order                 12 non-null     bool          
dtypes: bool(4), datetime64[ns](4), float64(1), int64(2), object(7)
```

`items_df` is a Pandas DataFrame with the following columns and types

```
Data columns (total 13 columns):
 #   Column                  Non-Null Count  Dtype  
---  ------                  --------------  -----  
 0   amazon_order_id         12 non-null     object 
 1   asin                    12 non-null     object 
 2   is_gift                 12 non-null     bool   
 3   item_price              12 non-null     float64
 4   item_tax                12 non-null     float64
 5   promotion_discount      12 non-null     float64
 6   promotion_discount_tax  12 non-null     float64
 7   quantity_ordered        12 non-null     int64  
 8   quantity_shipped        12 non-null     int64  
 9   seller_sku              12 non-null     object 
 10  shipping_price          3 non-null      float64
 11  shipping_tax            3 non-null      float64
 12  shipping_discount       3 non-null      float64
dtypes: bool(1), float64(7), int64(2), object(3)
```

`orders_df` and `items_df` may contain `None` and `np.nan` values. 

You can specify additional keyword arguments besides `timezone` to adjust rate processing settings: `request_pause_time`, `request_burst_size`, and `burst_pause_time`. For instance, a customized retrieval object could be:

```
custom_obj = SP_Orders_Retrieval(NA_creds, EUR_creds, rates, burst_pause_time=60, request_pause_time=3)
```

## Future Development

Updates will primarily be made to `sql_lib.py` for ease-of-use methods as it has been in use for some time. Updates to `amazon_sp_lib.py` on the other hand will be primarily for bug fixes or rate changes.

## Built With

* [Python 3.6.9](https://www.python.org/downloads/) - Python version. View the list of versions on the website.
* [Google Colaboratory](https://colab.research.google.com) - Online IDE used for notebook development.
* [Visual Studio Code](https://code.visualstudio.com/) - Local IDE.
* [pdoc3](https://pdoc3.github.io/pdoc/) - documentation package, one of [many](https://wiki.python.org/moin/DocumentationTools).

## Authors

* **Chami Lamelas** - *Developer* - 
[my personal GitHub](https://github.com/ChamiLamelas)

## Acknowledgments

* [PurpleBooth](https://github.com/PurpleBooth) - wrote the template for this file
* [gitignore](https://github.com/github/gitignore/blob/master/Python.gitignore) - gitignore file for Python files
