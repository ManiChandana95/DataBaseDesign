This is a project where a rudimentary database that is loosely based on MySQL is implemented.
Each database table will be stored physically as a separate single file. Each database table file will be subdivided into logical sections of fixed equal size call pages. Therefore, each table file size will be exact increments of the global page_size attribute, i.e. all data files must share the same page_size attribute. The page_size for my project is 512 Bytes.

Supported Commands:

• SHOW TABLES; – Displays a list of all tables in DavisBase.
• CREATE TABLE; – Creates a new empty table. 
• DROP TABLE table_name; – Removes a table schema, and all of its contained data.
• INSERT INTO table_name [column_list] VALUES [value_list]; - Inserts a single record into a table. 
• DELETE FROM table_name [WHERE condition]; - Deletes one or more records from a table.
• UPDATE table_name SET column_name = value WHERE [condition]; - Modifies one or more records in a table. 
• EXIT; – Cleanly exits the program and saves all table information in non-volatile files to disk.
