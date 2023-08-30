import pymysql
import os
from dotenv import load_dotenv

load_dotenv()
HOST = os.environ.get("mysql_host")
USER = os.environ.get("mysql_user")
PASSWORD = os.environ.get("mysql_pass")
WAREHOUSE_DB_NAME = os.environ.get("mysql_db")


def setup_db_connection(host=HOST, user=USER, password=PASSWORD, warehouse_db_name=WAREHOUSE_DB_NAME):
    print("connecting to database....")
    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=warehouse_db_name
    )
    cursor = connection.cursor()
    return connection, cursor


def load_data_from_sql(sql_query, cursor):
    cursor.execute(sql_query)
    result = cursor.fetchone()
    return result


def save_data_to_sql(*args, sql_code, cursor):
    cursor.execute(sql_code, args)
    generated_id = cursor.lastrowid
    return generated_id


def update_to_sql(sql_query, cursor):
    cursor.execute(sql_query)
################## Product ##############################


def is_product_in_sql(product_name, cursor):
    sql = f'SELECT * FROM product WHERE product_name="{product_name}";'
    # print(sql)
    # input("")
    cursor.execute(sql)
    result = cursor.fetchone()
    return result


def insert_product_into_sql(product_name, product_cost, cursor):
    sql = "INSERT INTO product (product_name, product_cost) VALUES (%s, %s);"
    values = (product_name, product_cost)
    cursor.execute(sql, values)

################# Branch ####################################

def is_branch_in_sql(branch_name, cursor):
    sql = f'SELECT * FROM branch WHERE branch_name = %s;'
    cursor.execute(sql, (branch_name,))
    result = cursor.fetchone()
    return result
################################################################

def insert_branch_into_sql(branch_name, cursor):
    sql = "INSERT INTO branch (branch_name) VALUES (%s);"
    values = (branch_name,)
    # cursor = db_connection.cursor()
    cursor.execute(sql, values)
    # db_connection.commit()
    # cursor.close()

############################ payment_method ############################

def is_payment_method_in_sql(payment_name, cursor):
    sql = f'SELECT * FROM payment_method WHERE payment_name="{payment_name}";'
    cursor.execute(sql)
    result = cursor.fetchone()
    return result


def insert_payment_method_into_sql(payment_name, cursor):
    sql = "INSERT INTO payment_method (payment_name) VALUES (%s);"
    values = (payment_name,)
    cursor.execute(sql, values)

##############################################################


def insert_csv_to_raw_data(raw_list):
    sql = """
            INSERT INTO raw_data (OrderDateTime, branch, Items, TotalAmount, PaymentMethod)
            VALUES (%s, %s, %s, %s, %s);
        """

    the_connection, my_cursor = setup_db_connection()
    for data in raw_list:
        row = (data['date'], data['branch'], data['order_details'],
               data['total'], data['mode_of_payment']
               )

        my_cursor.execute(sql, row)
    the_connection.commit()
    print('Rows inserted.')


def create_test_db_tables(connection, cursor):

    create_test_branch_table = \
        """
        
        CREATE TABLE IF NOT EXISTS test_branch (
            branch_id INT AUTO_INCREMENT PRIMARY KEY,
            branch_name VARCHAR(255) NOT NULL
        )
    
    """
    create_test_payment_method_table = \
        """
         CREATE TABLE IF NOT EXISTS test_payment_method (
            payment_id INT AUTO_INCREMENT PRIMARY KEY,
            payment_name VARCHAR(255) NOT NULL
        )
    
    """
    cursor.execute(create_test_branch_table)
    cursor.execute(create_test_payment_method_table)
    connection.commit()


def create_db_tables(connection, cursor):
    print("check if database exist and if not it will create...")
    create_branch_data_table = \
        """
        CREATE TABLE IF NOT EXISTS branch(
            branch_id INT NOT NULL AUTO_INCREMENT,
            branch_name varchar(50),
            PRIMARY KEY(branch_id)
        );
    """
    create_payment_method_table = \
        """
        CREATE TABLE IF NOT EXISTS payment_method(
            payment_id INT NOT NULL AUTO_INCREMENT,
            payment_name varchar(50),
            PRIMARY KEY(payment_id)
        );
    """
    create_products_table = \
        """
        CREATE TABLE IF NOT EXISTS product(
            product_id INT NOT NULL AUTO_INCREMENT,
            product_name varchar(50),
            product_cost decimal(19,2),
            PRIMARY KEY(product_id)
        );
    """
    create_transaction_table = \
        """
        CREATE TABLE IF NOT EXISTS transaction(
            transaction_id INT NOT NULL AUTO_INCREMENT,
            transaction_date varchar(50),
            branch_id int,
            total_payment decimal(19,2),
            payment_id int,
            PRIMARY KEY(transaction_id),
            FOREIGN KEY(branch_id) REFERENCES branch(branch_id),
            FOREIGN KEY(payment_id) REFERENCES payment_method(payment_id)
        );
    """
    create_order_details_table = \
        """
        CREATE TABLE IF NOT EXISTS order_details(
            Order_id INT NOT NULL AUTO_INCREMENT,
            transaction_id int,
            product_id int,
            quantity float,
            PRIMARY KEY(Order_id),
            FOREIGN KEY(transaction_id) REFERENCES transaction(transaction_id),
            FOREIGN KEY(product_id) REFERENCES product(product_id)
        );
    """

    cursor.execute(create_branch_data_table)
    cursor.execute(create_payment_method_table)
    cursor.execute(create_products_table)
    cursor.execute(create_transaction_table)
    cursor.execute(create_order_details_table)
    connection.commit()
    # cursor.close()
    # connection.close()
