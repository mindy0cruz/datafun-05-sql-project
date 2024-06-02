"""
Project 5
Mindy Cruz
6/2/2024

Summary:
Project 5 integrates Python and SQL, focusing on database interactions using SQLite. This project introduces logging, a useful tool for 
debugging and monitoring projects, and involves creating and managing a database, building a schema, and performing various SQL operations, 
including queries with joins, filters, and aggregations.
"""

# Import Dependencies
import sqlite3
import pandas as pd
import pathlib

# Logging
import logging

# Configure logging to write to a file, appending new logs to the existing file
logging.basicConfig(filename='log.txt', level=logging.DEBUG, filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("Program started")  # Add this at the beginning of the main method
logging.info("Program ended")  # Add this at the end of the main method

# Root Project Folder
db_file = pathlib.Path("project.db")

# Create
def insert_data_from_csv():
    """Function: use pandas to read data from CSV files (in 'data' folder) and 
    insert the records into their respective tables"""
    try:
        author_data_path = pathlib.Path("data", "authors.csv")
        book_data_path = pathlib.Path("data", "books.csv")
        authors_df = pd.read_csv(author_data_path)
        books_df = pd.read_csv(book_data_path)
        with sqlite3.connect(db_file) as conn:
            # Use the pandas DataFrame to_sql() method to insert data
            authors_df.to_sql("authors", conn, if_exists="replace", index=False)
            books_df.to_sql("books", conn, if_exists="replace", index=False)
            print("Data inserted successfully.")
    except (sqlite3.Error, pd.errors.EmptyDataError, FileNotFoundError) as e:
        print("Error inserting data from CSV:", e)
        logging.error("Error inserting data from CSV: %s", e)

# Insert
def insert_records():
    """Function to read and execute SQL statements to insert data"""
    try:
        with sqlite3.connect(db_file) as conn:
            insert_records_sql = pathlib.Path("sql", "insert_records.sql")
            with open(insert_records_sql, "r") as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print("Data inserted successfully.")
    except sqlite3.Error as e:
        print("Error inserting data from SQL:", e)
        logging.error("Error inserting data from SQL: %s", e)

# Update
def execute_update_records():
    """Function for updating records in the data tables"""
    try:
        with sqlite3.connect(db_file) as conn:
            update_records_sql = pathlib.Path("sql", "update_records.sql")
            with open(update_records_sql, 'r') as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print(f"Executed SQL from {update_records_sql}")
    except sqlite3.Error as e:
        print("Error updating records from SQL:", e)
        logging.error("Error updating records from SQL: %s", e)

# Delete
def execute_delete_records():
    """Function for deleting records from the data tables"""
    try:
        with sqlite3.connect(db_file) as conn:
            delete_records_sql = pathlib.Path("sql", "delete_records.sql")
            with open(delete_records_sql, 'r') as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print(f"Executed SQL from {delete_records_sql}")
    except sqlite3.Error as e:
        print("Error deleting records from SQL:", e)
        logging.error("Error deleting records from SQL: %s", e)

# Query Aggregation
def execute_query_aggregation():
    """Function for performing aggregation of data from tables"""
    try:
        with sqlite3.connect(db_file) as conn:
            query_aggregation_sql = pathlib.Path("sql", "query_aggregation.sql")
            with open(query_aggregation_sql, 'r') as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print(f"Executed SQL from {query_aggregation_sql}")
    except sqlite3.Error as e:
        print("Error executing query aggregation from SQL:", e)
        logging.error("Error executing query aggregation from SQL: %s", e)

# Query Filter
def execute_query_filter():
    """Function for filtering data within the tables"""
    try:
        with sqlite3.connect(db_file) as conn:
            query_filter_sql = pathlib.Path("sql", "query_filter.sql")
            with open(query_filter_sql, 'r') as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print(f"Executed SQL from {query_filter_sql}")
    except sqlite3.Error as e:
        print("Error executing query filter from SQL:", e)
        logging.error("Error executing query filter from SQL: %s", e)

# Query Sorting
def execute_query_sorting():
    """Function for sorting data within the tables"""
    try:
        with sqlite3.connect(db_file) as conn:
            query_sorting_sql = pathlib.Path("sql", "query_sorting.sql")
            with open(query_sorting_sql, 'r') as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print(f"Executed SQL from {query_sorting_sql}")
    except sqlite3.Error as e:
        print("Error executing query sorting from SQL:", e)
        logging.error("Error executing query sorting from SQL: %s", e)

# Query Grouping
def execute_query_group():
    """Function for grouping data within the tables"""
    try:
        with sqlite3.connect(db_file) as conn:
            query_group_sql = pathlib.Path("sql", "query_group.sql")
            with open(query_group_sql, 'r') as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print(f"Executed SQL from {query_group_sql}")
    except sqlite3.Error as e:
        print("Error executing query group by from SQL:", e)
        logging.error("Error executing query group by from SQL: %s", e)

# Query Join
def execute_query_join():
    """Function for joining data from 2 separate tables"""
    try:
        with sqlite3.connect(db_file) as conn:
            query_join_sql = pathlib.Path("sql", "query_join.sql")
            with open(query_join_sql, 'r') as file:
                sql_script = file.read()
            conn.executescript(sql_script)
            print(f"Executed SQL from {query_join_sql}")
    except sqlite3.Error as e:
        print("Error executing query join from SQL:", e)
        logging.error("Error executing query join from SQL: %s", e)

# Main Method
if __name__ == "__main__":

    logging.info("Program started")
    # Insert CSV data
    insert_data_from_csv()
    # Insert records from SQL
    insert_records()
    # Execute other operations as needed
    execute_update_records()
    execute_delete_records()
    execute_query_aggregation()
    execute_query_filter()
    execute_query_sorting()
    execute_query_group()
    execute_query_join()
    logging.info("Program ended")

