import csv
import re
import os
import sys
from datetime import datetime

from database_util_sql import (
    setup_db_connection,
    create_db_tables,
    is_branch_in_sql,
    insert_branch_into_sql,
    is_product_in_sql,
    insert_product_into_sql,
    is_payment_method_in_sql,
    insert_payment_method_into_sql,
    load_data_from_sql,
    save_data_to_sql,
    update_to_sql,
)


# Reads all rows from a CSV file and returns them as a list of lists.
def read_data_from_csv(filename):
    print(f"read_data_from_csv: started: filename={filename}")
    with open(filename, "r") as file:
        data = list(csv.reader(file))
    print(f"read_data_from_csv: done: filename={filename}")
    return data


# Parses the "Items" column from the CSV into a list of [product_name, price] pairs.
# Example input: "Regular Latte - 2.50, Large Latte - 3.10"
def get_products_items(product_items):
    print(f"get_products_items: started: product_items={product_items}")
    product_item_list = product_items.split(",")
    result = []

    for product in product_item_list:
        # Extracts "name - price" pairs using a regex.
        matches = re.findall(r"([\w\s-]+)\s*-\s*([\d.]+)", product.strip())

        for name, price in matches:
            cleaned_name = name.strip()
            result.append([cleaned_name, price])

    print("get_products_items: done")
    return result


# Returns the branch_id for a given branch name, or None if not found.
def get_branch_id(branch_name, cursor):
    sql = f'SELECT * FROM branch WHERE branch_name="{branch_name}";'
    get_branch = load_data_from_sql(sql, cursor)
    if get_branch:
        return get_branch[0]
    else:
        return None


# Returns the payment_id for a given payment method, or None if not found.
def get_payment_method_id(payment_name, cursor):
    sql = f'SELECT * FROM payment_method WHERE payment_name="{payment_name}";'
    get_payment_method = load_data_from_sql(sql, cursor)
    if get_payment_method:
        return get_payment_method[0]
    else:
        return None


# Inserts or updates order_details rows for all products belonging to a single transaction.
def process_product_items(transaction_id, product_items, cursor):
    product_item_list = product_items.split(",")
    for product in product_item_list:
        print("Inserting product ...")
        # Extracts product name and price.
        matches = re.findall(r"([\w\s-]+)\s*-\s*([\d.]+)", product)
        result = []
        for item, price in matches:
            clean_item_name = item.strip()
            cleaned_item = [clean_item_name, price]
            result.append(cleaned_item)

        if not result:
            continue

        product_name = result[0][0]
        product_cost = float(result[0][1])

        # Looks up product_id.
        sql = f'SELECT * FROM product WHERE product_name="{product_name}";'
        get_product = load_data_from_sql(sql, cursor)
        if get_product:
            product_id = get_product[0]
        else:
            product_id = None

        # Checks if this transaction/product combination already exists.
        sql = f"""
            SELECT * FROM order_details
            WHERE transaction_id="{transaction_id}" AND product_id="{product_id}";
        """
        isfound = load_data_from_sql(sql, cursor)

        if not isfound:
            # Inserts a new order_details row with quantity = 1.
            quantity = 1
            sql = """
                INSERT INTO order_details (transaction_id, product_id, quantity)
                VALUES (%s, %s, %s)
            """
            save_data_to_sql(transaction_id, product_id, quantity, sql_code=sql, cursor=cursor)
        else:
            # Increments the quantity if the row already exists.
            quantity = isfound[3] + 1
            sql = f"""
                UPDATE order_details
                SET quantity = '{quantity}'
                WHERE Order_id = '{isfound[0]}'
            """
            update_to_sql(sql, cursor)


# Ensures that all products from the CSV exist in the product dimension table.
def product_checking(data, connection, cursor):
    for row in data:
        products = row[3]
        product_item_list = get_products_items(products)
        for product_name, product_cost in product_item_list:
            if not is_product_in_sql(product_name, cursor):
                print("Inserting product data from CSV...")
                insert_product_into_sql(product_name, product_cost, cursor)
            else:
                print(f"Processing product... {product_name}")
    connection.commit()


# Ensures that all branches from the CSV exist in the branch dimension table.
def branch_checking(data, connection, cursor):
    for row in data:
        branch_name = row[1]
        if not is_branch_in_sql(branch_name, cursor):
            print("Inserting branch data from CSV...")
            insert_branch_into_sql(branch_name, cursor)
            connection.commit()
        else:
            print(f"Processing branch... {branch_name}")


# Ensures that all payment methods from the CSV exist in the payment_method dimension table.
def payment_method_checking(data, connection, cursor):
    print("payment_method_checking: started")
    for row in data:
        payment_name = row[5]
        if not is_payment_method_in_sql(payment_name, cursor):
            print("Inserting payment method data from CSV...")
            insert_payment_method_into_sql(payment_name, cursor)
        else:
            print(f"Processing payment method... {payment_name}")
    connection.commit()
    print("payment_method_checking: finished")


# Inserts transactions and related order_details for all rows in the CSV.
def transaction_checking_exist(data, cursor):
    print(f"transaction_checking_exist: started: data rows={len(data)}")
    for row in data:
        branch_name = row[1]
        payment_name = row[5]

        branch_id = get_branch_id(branch_name, cursor)
        payment_id = get_payment_method_id(payment_name, cursor)

        transaction_date = clean_and_format_date(row[0])

        sql = """
            INSERT INTO transaction (transaction_date, branch_id, total_payment, payment_id)
            VALUES (%s, %s, %s, %s)
        """
        generated_transaction_id = save_data_to_sql(
            transaction_date,
            branch_id,
            row[4],
            payment_id,
            sql_code=sql,
            cursor=cursor,
        )

        process_product_items(generated_transaction_id, row[3], cursor)

    print(f"transaction_checking_exist: done: data rows={len(data)}")


# Cleans and converts a date/time string into the expected date format.
def clean_and_format_date(
    date_str, current_format="%d/%m/%Y %H:%M", expected_format="%d/%m/%Y"
):
    print(f"clean_and_format_date: started: date_str={date_str}")
    try:
        date_time = datetime.strptime(date_str, current_format)
    except ValueError:
        print(f"Invalid date format: {date_str}")
        return None

    formatted_date = date_time.strftime(expected_format)
    print(f"clean_and_format_date: done: formatted_date={formatted_date}")
    return formatted_date


# Entry point: processes all CSV files in the Data/ directory
# and loads them into the warehouse database.
if __name__ == "__main__":
    print("main: started")

    directory_name = "Data"
    current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    directory_path = os.path.join(current_dir, directory_name)

    # Sets up database connection and ensures all tables exist.
    connection, cursor = setup_db_connection()
    create_db_tables(connection, cursor)

    file_list = os.listdir(directory_path)
    for filename in file_list:
        if filename.endswith(".csv") and "done" not in filename:
            file_path = os.path.join(directory_path, filename)
            data = read_data_from_csv(file_path)

            # Loads dimension and fact tables from this CSV file.
            product_checking(data, connection=connection, cursor=cursor)
            branch_checking(data, connection=connection, cursor=cursor)
            payment_method_checking(data, connection=connection, cursor=cursor)
            transaction_checking_exist(data, cursor=cursor)

            # Commits after fully processing a file.
            connection.commit()

            # Renames the processed file to mark it as done.
            done_file_name = filename.replace(".csv", "_done.csv")
            done_file_path = os.path.join(directory_path, done_file_name)
            os.rename(file_path, done_file_path)
            print(f"CSV file processed and renamed: {done_file_path}")
        else:
            print(f"\n[{filename}]\n Imported DONE already...\n")

    print("main: done")
    print("Processed successfully.\n")

    cursor.close()
    connection.close()