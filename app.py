# SUPER CAFE PROJECT
# The Problem
# • The software currently being used only generates reports for
# single branches.
# • It is time consuming to collate data on all branches.
# • Gathering meaningful data for the company on the whole is
# difficult, due to the limitations of the software.

# The Bigger Problem
# The company currently has no way of identifying trends, meaning
# they are potentially losing out on major revenue streams.
# They are in desperate need of help putting together a platform that
# will allow them to easily understand all of the data they are producing.
# Due to the highly professional work you completed for them in the
# past, they are keen to work alongside you in creating a solution to
# solve the problem they're facing.

# Team coffeeoverflowerror
# Members:
# Kirian
# Kevin
# Liam
# Jewen


import csv
import re
import os
import sys
from datetime import datetime
from database_util_sql import setup_db_connection, create_db_tables
from database_util_sql import is_branch_in_sql, insert_branch_into_sql
from database_util_sql import is_product_in_sql, insert_product_into_sql
from database_util_sql import is_payment_method_in_sql, insert_payment_method_into_sql
from database_util_sql import load_data_from_sql, save_data_to_sql, update_to_sql


def read_data_from_csv(filename):
    print(f"read_data_from_csv: started: filename={filename}")
    with open(filename, 'r') as file:
        data = list(csv.reader(file))

    print(f"read_data_from_csv: done: filename={filename}")
    return data


def get_products_items(product_items):
    print(f"get_products_items: started: product_items={product_items}")
    product_item_list = product_items.split(',')
    result = []
    for product in product_item_list:
        # print(f'before {product}')
        # input('')
        my_split = re.findall(r"([\w\s-]+)\s*-\s*([\d.]+)", product.strip())
        # print(my_split)
        # input('')
        result = []
        for product, price in my_split:
            cleaning_item_name = product.strip()
            # print(cleaning_item_name)
            # input('')
            cleaned_item = [cleaning_item_name, price]
            # print(cleaned_item)
            # input('')
            result.append(cleaned_item)

    print(f"get_products_items: done")
    return result


def get_branch_id(branch_name, cursor):
    sql = f'SELECT * FROM branch WHERE branch_name="{branch_name}";'
    get_branch = load_data_from_sql(sql, cursor)
    if get_branch:
        return get_branch[0]
    else:
        return None


def get_payment_method_id(payment_name, cursor):
    sql = f'SELECT * FROM payment_method WHERE payment_name="{payment_name}";'
    get_payment_method = load_data_from_sql(sql, cursor)
    if get_payment_method:
        return get_payment_method[0]
    else:
        return None


def process_product_items(transaction_id, product_items, cursor):
    product_item_list = product_items.split(',')
    for product in product_item_list:
        print("Inserting procdct ....")
        my_split = re.findall(r"([\w\s-]+)\s*-\s*([\d.]+)", product)
        result = []
        for item, price in my_split:
            clean_item_name = item.strip()
            cleaned_item = [clean_item_name, price]
            result.append(cleaned_item)
        product_name = result[0][0]
        product_cost = float(result[0][1])

        sql = f'SELECT * FROM product WHERE product_name="{product_name}";'
        get_product = load_data_from_sql(sql, cursor)
        if get_product:
            product_id = get_product[0]
        else:
            product_id = None

        sql = f'SELECT * FROM order_details WHERE transaction_id="{transaction_id}" and product_id="{product_id}";'
        isfound = load_data_from_sql(sql, cursor)

        if not isfound:
            quantity = 1
            sql = """
                INSERT INTO order_details (transaction_id,product_id,quantity)
                VALUES (%s,%s,%s)
            """
            save_data_to_sql(transaction_id, product_id,
                             quantity, sql_code=sql, cursor=cursor)
        else:
            quantity = isfound[3] + 1
            sql = f"""
                UPDATE order_details SET quantity = '{quantity}' WHERE Order_id = '{isfound[0]}'
            """
            update_to_sql(sql, cursor)


def product_checking(data, connection, cursor):
    for row in data:
        procducts = row[3]
        product_item_list = get_products_items(procducts)
        for product_name, product_cost in product_item_list:
            if not is_product_in_sql(product_name, cursor):
                print("Inserting product Data from CSV...")
                insert_product_into_sql(
                    product_name, product_cost, cursor)
            else:
                print(f'Processing product... {product_name}')
    connection.commit()


def branch_checking(data, cursor):
    for row in data:
        branch_name = row[1]
        if not is_branch_in_sql(branch_name, cursor):
            print("Inserting branches Data from CSV..")
            insert_branch_into_sql(branch_name, cursor)
            connection.commit()
        else:
            print(f'Processing branch... {branch_name}')


def payment_method_checking(data, cursor):
    print(f'payment_method_checking: started')
    for row in data:
        if not is_payment_method_in_sql(row[5], cursor):
            print('Inserting payment methon Data from CSV...')
            insert_payment_method_into_sql(row[5], cursor)
        else:
            print(f'Processing payment method... {row[5]}')
    connection.commit()
    print(f'payment_method_checking: finished')


def transaction_checking_exist(data, cursor):
    print(f'transaction_checking_exist: started: data rows={len(data)}')
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
            transaction_date, branch_id, row[4], payment_id, sql_code=sql, cursor=cursor)
        process_product_items(generated_transaction_id, row[3], cursor)

    print(f'transaction_checking_exist: done: data rows={len(data)}')


def clean_and_format_date(date_str, current_format='%d/%m/%Y %H:%M', expected_format='%d/%m/%Y'):
    print(f'clean_and_format_date: started: date_str={date_str}')
    try:
        date_time = datetime.strptime(date_str, current_format)
    except ValueError:
        print(f"Invalid date format: {date_str}")
        return None
    formatted_date = date_time.strftime(expected_format)
    print(f'clean_and_format_date: done: formatted_date={formatted_date}')
    return formatted_date


if __name__ == "__main__":
    print('main: started')
    directory_name = 'Data'
    current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

    # directory_path = os.path.join(current_dir, '..', directory_name)
    directory_path = os.path.join(current_dir, directory_name)

    connection, cursor = setup_db_connection()
    create_db_tables(connection, cursor)
    file_list = os.listdir(directory_path)
    for filename in file_list:
        if filename.endswith('.csv') and 'done' not in filename:
            file_path = os.path.join(directory_path, filename)
            data = read_data_from_csv(file_path)
            ### save data into tables ######
            product_checking(data, connection=connection, cursor=cursor)
            branch_checking(data, cursor)
            payment_method_checking(data, cursor)
            transaction_checking_exist(data, cursor)
            ################################
            connection.commit()
            done_file_name = filename.replace('.csv', '_done.csv')
            done_file_path = os.path.join(directory_path, done_file_name)
            os.rename(file_path, done_file_path)
            print(f"CSV Files processed and renamed: {done_file_path}")
        else:
            print(f"\n[{filename}]\n Imported DONE already...\n")

    print('main: done')

print(f"Processed Successfully Done!!!\n")
cursor.close()
connection.close()


# result
# sale_date	branch_name	product_name	total_sales
# 2021-08-25	Chesterfield	Regular Flavoured iced latte - Hazelnut	181.5
# 2021-08-25	Chesterfield	Large Flavoured iced latte - Hazelnut	185.25
# 2021-08-25	Chesterfield	Regular Flavoured latte - Hazelnut	165.74999999999997
# 2021-08-25	Chesterfield	Large Flavoured latte - Hazelnut	205.1999999999998

# sql command
# SELECT
#     IFNULL(STR_TO_DATE(t.transaction_date, '%d/%m/%Y'), 'No Sales Date') AS sale_date,
#     b.branch_name,
#     p.product_name,
#     SUM(o.quantity) AS total_items_sold,
#     ROUND(SUM(o.quantity * p.product_cost), 2) AS total_sales
# FROM
#     product p
#     LEFT JOIN order_details o ON p.product_id = o.product_id
#     LEFT JOIN `transaction` t ON o.transaction_id = t.transaction_id
#     LEFT JOIN branch b ON t.branch_id = b.branch_id
# WHERE
#     p.product_name LIKE '%Hazelnut%'
# GROUP BY
#     sale_date, b.branch_name, p.product_name;


# sale_date	branch_name	total_sales_per_branch
# 2021-08-25	Chesterfield	2088.6500000000015
# 2023-05-09	Leeds	278.4499999999999


# SELECT
#     IFNULL(STR_TO_DATE(t.transaction_date, '%d/%m/%Y'), 'No Sales Date') AS sale_date,
#     b.branch_name,
#     SUM(o.quantity * p.product_cost) AS total_sales_per_branch
# FROM
#     product p
#     JOIN order_details o ON p.product_id = o.product_id
#     JOIN `transaction` t ON o.transaction_id = t.transaction_id
#     JOIN branch b ON t.branch_id = b.branch_id
# GROUP BY
#     sale_date, b.branch_name;
