"""
This file contains a group of classes to abstract pymysql operations. There are 3 primary ones:

* The first is `MySQL_DB_Connection` which abstracts making a connection a MySQL database for performing queries. 
* The second is `MySQL_Table_Column` which provides a represntation of a column in a schema. You can set the name, data type, whether it can be null, and extra attributes.
* The third is `MySQL_Table_Schema` used for quickly creating a table over a connection, checking if an instance exists on a schema, and if all integrity constraints are satisfied for a given instance. 
"""

# Author: Chami Lamelas
# Last updated: 2/9/2021

import pymysql.cursors
import socket
import pandas as pd
from enum import Enum

class MySQL_DB_Connection:
    """
    This class represents a connection to a MySQL database. 
    
    It serves to wrap the various opening and clean up activities that must be performed in order to perform queries. 

    Attributes
    ----------
    host : str
        Host where the database server is located for which the connection should be established.
    user : str 
        Username to log in as to make connection.
    password : str
        Password to use to make connection 
    port : int
        MySQL port to use to connect, default (3306) is usually OK. 
    database : 
        Database to use to connect

    Parameters
    ----------
    host : str
        Host where the database server is located for which the connection should be established.
    user : str 
        Username to log in as to make connection.
    password : str
        Password to use to make connection 
    port : int
        MySQL port to use to connect, default (3306) is usually OK. 
    database : 
        Database to use to connect

    References
    ----------
    [Connections in pymysql](https://pymysql.readthedocs.io/en/latest/modules/connections.html)
    """

    READ_QUERY_KEYWORDS = ['SELECT', 'SHOW', 'DESCRIBE']
    """
    List of MySQL keywords for reading any information from a database.  
    """

    DEFAULT_READ_LIMIT = 5
    """
    Default number of rows returned in read query.
    """

    def __init__(self, host, user, password, port, database):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port        

    def query(self, q, args=None, update_many=False):
        """
        Runs a query on the connected database with the parameters set either by the constructor.

        Parameters
        ----------
        q : str
            The query to be run on the database. Any argument values (as in INSERT, REPLACE, or DELETE commands) should be marked with %s and specified in the args parameter. 
        args : iterable, optional
            In the case of a single row being updated or for the parameter of a deletion, this will be an iterable such as a list or tuple. If a collection or rows are being added or updated, this will be a list of iterables and in this case `update_many` should be set to true. The default value is None.
        update_many : bool, optional
            When many rows are to be updated via INSERT or REPLACE, mark this parameter as true to improve update performance. The default value is False.

        Returns
        -------
        result : int or iterable
            If a read type query was performed (specified by READ_QUERY_KEYWORDS), the read results are returned (could be rows of a table with SELECT or table information via SHOW, DESCRIBE). Otherwise, in the case of an update to the database, the number of updated rows (int) are returned.

        Raises
        ------
        RuntimeError
            If the query fails in any way. One possible error is a pymysql.err.IntegrityError if a duplicate
            entry tries to be inserted, it will have an error code (as per MySQL standard) of 1062.

        References
        -----
        [Wikipedia Article on Cursors](https://en.wikipedia.org/wiki/Cursor_(databases))

        [Examples of pymysql](https://pymysql.readthedocs.io/en/latest/user/examples.html)

        [Cursors in pymysql](https://pymysql.readthedocs.io/en/latest/modules/cursors.html)
        """

        result = None

        # Establish connection to DB using pymysql
        connection = pymysql.connect(
            host=self.host, user=self.user, password=self.password, port=self.port, database=self.database)
        try:

            # Obtain cursor over Database and perform clean-up in any exit (exception or not) 
            with connection.cursor() as cursor:

                # Determines if we are doing a read query by seeing if query starts with one of keywords
                doing_read_query = any(
                    [q.startswith(r) for r in MySQL_DB_Connection.READ_QUERY_KEYWORDS])

                # If we are doing an update query, and more specifically one that makes many updates, uses
                # executemany() which provides better performance for multiple REPLACE or INSERT
                if not doing_read_query and update_many:
                    result = cursor.executemany(q, args)

                # Otherwise, if we are doing a single update or a read query, run a regular execute()
                else:
                    result = cursor.execute(q, args)

                # If doing a read query, will be returning the corresponding records as opposed to the number
                # of rows that are updated (would be 0 for a read query).
                if doing_read_query:
                    result = cursor.fetchall()

                # Otherwise, will be returning number of updated rows but need to commit the update over the
                # connection
                else:
                    connection.commit()
                return result
                
        # Make sure connection is closed as clean up
        finally:
            connection.close()

    def insert(self,table,data,schema_cols=None,overwrite=False,status_check=True,constraint_check=True):
        """
        Inserts the rows from a DataFrame onto a table on this database. By default, matching rows are not overwritten and a safety check is performed to ensure the table is on the database. 

        Parameters
        ----------

        table : MySQL_Table_Schema
            Schema object representing table to be updated
        data : list or DataFrame  
            Insertion data
        schema_cols : list, optimal
            Names of schema columns that are being inserted via `data`. By default, this is `table.get_column_names()`. 
        overwrite : bool, optional
            Whether to do overwrite (False -> MySQL INSERT, True -> MySQL REPLACE). By default, this is true. Note that if you have an auto incrementing primary key or if you are inserting non primary key columns, SQL INSERTS can still lead to duplicates (i.e. when overwite=False). 
        status_check : bool, optional
            Whether to check `table` status on `database`. By default, this is true.
        constraint_check : bool, optional
            Whether to check `table` constraints after insert. By default, this is true.

        Returns
        -------
        q_res : int or iterable
            Result of insertion if successful. 
        
        Raises
        ------
        ValueError
            If DataFrame cannot be inserted (column mismatch), if `table` not on `database`, or insertion violated `table` constraints.
        RuntimeError
            If `MySQL_DB_Connection.query` fails.

        Notes
        -----
        `schema_cols` should be a subset of the tables columns to make sure that the DataFrame you are uploading is valid for the schema. Don't just pass in `data.columns` for instance. You should use this to validate a DataFrame being built from data created by users.

        See Also
        --------
        MySQL_DB_Connection.query
        MySQL_Table_Schema
        """

        if not (isinstance(data, pd.DataFrame) or isinstance(data, list)):
          raise TypeError('data (type: %s) must be either a list (to represent 1 row) or a DataFrame (to represent many rows)' % (type(data)))

        if not schema_cols:
            schema_cols = table.get_column_names()
        if isinstance(data, pd.DataFrame):
          df_cols = list(data.columns)
          if len(schema_cols)!=len(df_cols) or any(schema_cols[i]!=df_cols[i] for i in range(len(df_cols))):
            raise ValueError('Cannot insert DataFrame into table, columns do not match.')
        elif len(schema_cols) != len(data):
          raise ValueError('Cannot insert row into table, number of columns do not match.')
          
        if status_check:
          table.check_on_db(self)
        cols_to_update = ",".join("`{0}`".format(c) for c in schema_cols)
        arg_placeholders = ("%s,"*len(schema_cols))[:-1]
        query_template = "%s INTO `%s` (%s) VALUES(%s)" % ('REPLACE' if overwrite else 'INSERT', table.name, cols_to_update, arg_placeholders)
        update_type = type(data) == pd.DataFrame
        ins_args = data.values.tolist() if update_type else data
        q_res = self.query(query_template,args=ins_args,update_many=update_type)
        if constraint_check:
          table.check_constraints_on_db(self)
        return q_res

    def read(self,table,columns=None,where=None,limit=0,status_check=True):
        """
        Performs a read on a provided table on the DB. One can specify columns, where clause, and limit. 

        Parameters
        ----------
        table : MySQL_Table_Schema 
            Table to read from
        columns : list, optional 
            List of columns to retrieve (None/[] -> all columns). By default, it is None.
        where : str, optional
            Where clause (None -> no where clause), must be valid MySQL. By default, it is None.
        limit : int, optional
            Limit (None -> no limit), limit < 1 => limit rows to 5. By default, it is 0.
        status_check : bool, optional
            Whether to check `table` status on `database`. By default, it is true.

        Returns
        -------
        df : DataFrame
            Result of read if successful.

        Raises
        ------
        ValueError
            If `table` not on `database`.
        RuntimeError 
            If `MySQL_DB_Connection.query` fails.

        Warnings
        --------
        Be careful with what you allow a user to pass into `where` as it accepts any valid SQL and is vulnerable to a SQL injection.

        See Also
        --------
        MySQL_DB_Connection.query
        MySQL_Table_Schema        
        """

        if limit is not None and (where is None or limit<1):
            limit=MySQL_DB_Connection.DEFAULT_READ_LIMIT
          
        if status_check:
          table.check_on_db(self)
        cols_s = '*' if not columns else ','.join(columns)
        where_s = ' WHERE %s' % (where) if where is not None else ''
        limit_s = ' LIMIT %d' % (limit) if limit is not None else ''
        query_template='SELECT %s FROM `%s`%s%s' % (cols_s,table.name,where_s,limit_s)
        df = pd.DataFrame(self.query(query_template),columns=columns if columns else table.get_column_names())
        return df

    def key_get(self,table,key_values,status_check=True):
        """
        Performs a read on a provided table on the `database` and retrieves associated rows.

        Parameters
        ----------
        table : MySQL_Table_Schema
            Table to read from
        key_values : list 
            List of key values to match
        status_check : bool, optional 
            Whether to check `table` status on `database`. By default, it's true.

        Returns
        -------
        df : DataFrame
            `MySQL_DB_Connection.read` result on the provided `table` primary key 
            
        Raises
        ------
        ValueError 
            If `len(key_values) != len(table.primary_key)` or reasons specified in `MySQL_DB_Connection.read`.
        RuntimeError
            If `MySQL_DB_Connection.read` fails. 
        
        See Also
        --------
        MySQL_DB_Connection.read
        MySQL_Table_Schema
        """

        if not table.primary_key:
            raise ValueError('Input table has no primary key.')
        if len(key_values) != len(table.primary_key):
            raise ValueError("%d key values provided for primary key of length %d." % (len(key_values),len(table.priamry_key)))
        
        where_str = ' AND '.join(['%s=%s' % (table.primary_key[i],'\'%s\'' % (key_values[i]) if isinstance(key_values[i],str) else key_values[i]) for i in range(len(key_values))])
        df = self.read(table,None,where_str,0,status_check)
        return df

    def delete(self,table,where=None,delete_all=False,status_check=True,constraint_check=True):
        """
        Performs a deletion operation on a provided table on the DB. One can specify a where clause. If no where clause is supplied, as a security precuation (to ensure that the caller really wants to delete everything, they must specify delete_all=True)

        Parameters
        ----------
        table : MySQL_Table_Schema
            Table to delete from
        where : str, optional 
            Where clause (None -> no where clause, delete all), must be valid SQL. By default, it's None.
        delete_all : bool, optional
            If `where` is None, then `delete_all` must be True to perform a full deletion. If `where` is not None, `delete_all` is ignored. By default, it's false.
        status_check : bool, optional
            Whether to check `table` status on `database`. By default, it's true.
        constraint_check : bool, optional
            Whether to check `table` constraints after insert. By default, it's true.

        Returns
        -------
        res : int
            query result of the operation
            
        Raises
        ------
        ValueError 
            If `table` is not on `database` or if `table` constraint checks fail after the deletion.
        RuntimeError
            If MySQL_DB_Connection.query() fails. 

        Warnings
        --------
        Be careful with what you allow a user to pass into `where` as it accepts any valid SQL and is vulnerable to a SQL injection.
        
        See Also
        --------
        MySQL_DB_Connection.query()
        MySQL_Table_Schema
        """

        if not where and not delete_all:
            raise ValueError("To delete all rows, you must specify delete_all=True")
        
        if status_check:
          table.check_on_db(self)
        where_s = ' WHERE %s' % (where) if where is not None else ''
        query_template = 'DELETE FROM `%s`%s' % (table.name,where_s)
        res = self.query(query_template)
        if constraint_check:
          table.check_constraints_on_db(self)
        return res

    def get_schema(self, schema_name):
        """
        Given the name of a schema, this method will build a `MySQL_Table_Schema` object from the schema with the matching name on this connection.

        Parameters
        ----------
        schema_name : str
            Name of the schema from which to build schema object

        Returns
        -------
        `MySQL_Table_Schema` object constructed from the schema associated with `schema_name` on this connection

        Raises
        ------
        pymysql.err.ProgrammingError
            If no schema with name `schema_name` exists on the associated database of this connection (MySQL error code 1146)
        RuntimeError
            If `MySQL_DB_Connection.query` fails for some other reason

        Notes
        -----
        Any returned `MySQL_Table_Schema` object will not have any constraints as those are not stored on the database associated with the connection. You need to set the constraints yourself using the `constraints` attribute.
        """

        res = self.query("SHOW COLUMNS IN %s" % (schema_name))
        cols = []
        keys = []
        for c in res:
            cols.append(MySQL_Table_Column(c[0],c[1],c[2]=='YES',c[5] if len(c[5])>0 else None))
            if c[3] == 'PRI':
                keys.append(c[0])
        return MySQL_Table_Schema(schema_name, cols, keys)

class MySQL_Table_Status(Enum):
    """
    Enum that represents 3 status types of the existence of a table on a database. This is used by `MySQL_Table_Schema` when creating and checking the status of a table on a database.

    See Also
    --------
    MySQL_Table_Schema
    """

    TABLE_NOT_ON_DB = 1
    """
    Error status if schema does not exist on a database
    """

    TABLE_ON_DB_DIFF_COLS = 2
    """
    Error status if schema exists on a database but with different columns 
    """

    TABLE_ON_DB_DIFF_KEY = 3
    """
    Error status if schema exists on a database but with different primary key 
    """

class MySQL_Table_Column:
    """
    Represents a MySQL Table column. This is used in `MySQL_Table_Schema` for creating columns.

    Attributes
    ----------
    name : str
        Name of the column
    allow_null : bool
        Whether this column can have NULL values

    Parameters
    ----------
    name : str
        Name of the column
    dtype : str
        SQL data type of the column
    allow_null : bool, optional
        Whether this column can have NULL values
    extra : str, optional
        Extra information on the column

    See Also
    --------
    MySQL_Table_Schema
    """

    def __init__(self,name,dtype,allow_null=True,extra=None):
        self.name = name
        self.__dtype = dtype
        self.allow_null = allow_null
        self.__extra = extra

    def __eq__(self,other):
        if not isinstance(other,MySQL_Table_Column):
            return False
        return (
            self.name == other.name and
            self.__dtype == other.__dtype and
            self.allow_null == other.allow_null and
            self.__extra == other.__extra
        )

    def __ne__(self,other):
        return not self.__eq__(other)

    def to_sql(self):
        """
        Converts this column to a string that would be used in a SQL creation query. 

        Returns
        -------
        String representation of the column used in SQL.
        """

        out = self.name + " " + self.__dtype
        if not self.allow_null:
            out += " NOT NULL"
        if self.__extra:
            out += " " + self.__extra
        return out

class MySQL_Table_Constraint:
    """
    Represents a MySQL Table constraint. This is used in `MySQL_Table_Schema` for creating and enforcing constraints.

    Attributes
    ----------
    query : str
        The query used to check the constraint
    pass_val : object
        Output `query` should result in to signify the constraint passing
    fail_msg : str
        Failure message to display if `query` does not result in `pass_val`

    Parameters
    ----------
    query : str
        The query used to check the constraint
    pass_val : object
        Output `query` should result in to signify the constraint passing
    fail_msg : str, optional
        Failure message to display if `query` does not result in `pass_val`

    See Also
    --------
    MySQL_Table_Schema
    """

    def __init__(self,query,pass_val,fail_msg=""):
        self.query = query
        self.pass_val = pass_val
        self.fail_msg = fail_msg

class MySQL_Table_Schema:
    """
    Class that represents the schema of a MySQL table. This class offers functionality to create an instance of this schema on a database, check if an instance exists on a database, and check if all schema integrity constraints are upheld for a particular instance. 

    Attributes
    ----------
    name : str 
        Name of the schema 
    primary_key : list
        Column names that form a primary key over the schema
    constraints : list
        Constraints of the schema
    
    Parameters
    ----------
    name : str 
        Name of the schema 
    columns : list 
        Columns of the schema 
    primary_key : list or str, optional
        Column names that form a primary key over the schema. By default, it's None. 
    constraints : list, optional
        Constraints of the schema
    
    Warnings
    --------
    Be careful with what you pass into `constraints` as those objects accept any valid SQL and is vulnerable to a SQL injection.

    Notes
    -----
    If you do not want to provide any failure messages for the constraint checks, make sure you pass empty strings.
    Whatever you pass as the primary_key will become a list when you access it later.
    You can initialize a schema object from a schema with a specified name on a `MySQL_DB_Connection` using `MySQL_DB_Connection.get_schema`. Note, any schema created by this method will not have any constraints as those are not stored on the database associated with the connection. You can set the constraints yourself using the `constraints` attribute.

    See Also
    --------
    MySQL_Table_Column
    MySQL_Table_Constraint
    MySQL_DB_Connection.get_schema()
    """

    def __init__(self, name, columns, primary_key=None, constraints=[]):
        self.name = name
        self.__columns = columns
        self.primary_key = primary_key if isinstance(primary_key,list) else [primary_key]
        key_set = set(self.primary_key)
        for c in self.__columns:
            if c.name in key_set:
                c.allow_null = False
        self.constraints = constraints

    @staticmethod
    def __raise_exception(msg, detailed_err, err_type):
        raise ValueError(msg if detailed_err else err_type.value)

    def create_on_db(self, db_conn):
        """
        Initializes an instance of this schema on a database 

        Parameters
        ----------
        db_conn : MySQL_DB_Connection
            Connection to database where schema should be initialized 

        Returns
        -------
        res : int
            Result of creation query if one is made.

        Raises
        ------
        ValueError
            If the schema exists on `db_conn` with different columns or primary key.
        RuntimeError
            If an error occurs running this creation query.

        Notes
        -----
        If the schema (with the correct columns and primary key) exists on the connection, then nothing happens. If the schema does not exist on the connection, it is added as a new table on the connection. 

        See Also
        --------
        check_on_db
        """

        try:
            # Run check with error code response
            self.check_on_db(db_conn, False)
        except ValueError as e:

            # If table is not on database, create it
            if int(str(e)) == MySQL_Table_Status.TABLE_NOT_ON_DB.value:

                # Create table with name and primary key if one exists
                query = "CREATE TABLE " + self.name + " (" + ", ".join([c.to_sql() for c in self.__columns])
                if self.primary_key:
                    query += ", PRIMARY KEY(" + \
                        ", ".join(self.primary_key) + ")"
                query += ")"
                res = db_conn.query(query)
                return res
            else:
                # If schema is different on db
                raise ValueError("An instance of the schema {schema_name} already exists on database {db_name}.\nFor more information, run check_on_db() to compare invoking schema instance with schema on {db_name}.".format(
                    schema_name=self.name, db_name=db_conn.database))

    def check_on_db(self, db_conn, detailed_err=True):
        """
        Checks if there is an instance of this schema on a database

        Parameters
        ----------
        db_conn : MySQL_DB_Connection
            Database to check 
        detailed_err : bool, optional
            Whether to raise detailed error messages or not. By default it's True.

        Returns
        -------
        str :
            If the schema exists on a database, then the string representation of the schema is returned 

        Raises
        ------
        ValueError 
            If the schema does not exist, has different columns, or has a different primary key. If `detailed_err` = True, then the error is raised with a detailed message. Otherwise, an error code specified by `MySQL_Table_Status` is passed as the message.

        References
        ----------
        [MySQL Column Retrieval](https://dev.mysql.com/doc/refman/8.0/en/show-columns.html)

        See Also
        --------
        MySQL_Table_Status
        MySQL_DB_Connection
        """

        # Get tables with matching name
        table_on_db = db_conn.query("SHOW TABLES LIKE %s", args=self.name)

        # If none exist, return detailed error or with code 1 based on detailed_err
        if len(table_on_db) == 0:
            MySQL_Table_Schema.__raise_exception(
                "Instance of schema {0} is not on database.".format(self.name),
                detailed_err,
                MySQL_Table_Status.TABLE_NOT_ON_DB
            )

        # Get the columns the schema on the database (iterable of tuples)
        cols_in_db_table = db_conn.query("SHOW COLUMNS FROM " + self.name)

        cols_in_db = dict()
        for c in cols_in_db_table:
            # c[0]: name, c[1]: type, c[2]: null allowed (YES/NO), c[5]: extra
            cols_in_db[c[0]] = MySQL_Table_Column(
                c[0],
                c[1],
                c[2]=='YES',
                None if len(c[5])==0 else c[5]
            )

        if len(cols_in_db) != len(self.__columns):
            MySQL_Table_Schema.__raise_exception(
                "Number of columns of %s on %s does not match.\nInstance columns length: %d\nDatabase columns length: %d" % (
                    self.name,
                    db_conn.database,
                    len(self.__columns),
                    len(cols_in_db)
                ),
                detailed_err,
                MySQL_Table_Status.TABLE_NOT_ON_DB
            )

        for c in self.__columns:
            match = cols_in_db.get(c.name)
            if not match:
                MySQL_Table_Schema.__raise_exception(
                    "Column %s not on %s." % (c.name, db_conn.database),
                    detailed_err,
                    MySQL_Table_Status.TABLE_ON_DB_DIFF_COLS
                )
            if match != c:
                MySQL_Table_Schema.__raise_exception(
                    "Column %s has different properties on %s.\nSchema column:%s\nDatabase column:%s" % (c.name,db_conn.database,c.to_sql(),match.to_sql()),
                    detailed_err,
                    MySQL_Table_Status.TABLE_ON_DB_DIFF_COLS
                )

        # Collect primary key information from table as a list of tuples
        primary_key_in_db_table = db_conn.query(
            "SHOW KEYS FROM " + self.name + " WHERE Key_name = %s", args=('PRIMARY',))
        keys_in_db = {k[4] for k in primary_key_in_db_table}

        if len(keys_in_db) != len(self.primary_key):
            MySQL_Table_Schema.__raise_exception(
                "Number of primary key components of %s on %s does not match.\nInstance primary key length: %d\nDatabase primary key length: %d" % (
                    self.name,
                    db_conn.database,
                    len(self.primary_key),
                    len(keys_in_db)
                ),
                detailed_err,
                MySQL_Table_Status.TABLE_NOT_ON_DB
            )

        for k in self.primary_key:
            if k not in keys_in_db:
                MySQL_Table_Schema.__raise_exception(
                    "Element %s of primary key not found on %s." % (k, db_conn.database),
                    detailed_err,
                    MySQL_Table_Status.TABLE_ON_DB_DIFF_KEY
                )
        return str(self)

    def check_constraints_on_db(self, db_conn):
        """
        Checks if the instance of this schema on a database meets all the integrity constraints

        Parameters
        ----------
        db_conn : MySQL_DB_Connection 
            Database to check constraints 

        Raises
        ------
        ValueError
            If one of the constraints is violated (with a fail message , if a message exists in `constraint_fail_msgs`). 
        """

        # For each constraint query, run it and see if matches the pass value. If not a ValueError is raised with "" or the corresponding failure message
        for i in range(len(self.constraints)):
            res = db_conn.query(self.constraints[i].query)
            if res != self.constraints[i].pass_val:
                raise ValueError("Integrity constraint {0} violated.\nReceived: {1}\nExpected: {2}\nAdditional info: {3}".format(
                    i, res, self.constraints[i].pass_val, self.constraints[i].fail_msg))

    def get_column_names(self):
        return [c.name for c in self.__columns]

    def __str__(self):
        """
        Gets a string representation of the schema which includes the name, the columns and types, and the primary key. 

        Returns
        -------
        out : str
            A string representation of the schema. 
        """

        out = "NAME: %s\nCOLUMNS:\n" % (self.name)
        out += "\n".join(c.to_sql() for c in self.__columns)
        out += "\nPRIMARY KEY:" + ", ".join(self.primary_key) if self.primary_key else ""
        return out



