import mysql.connector
from dotenv import load_dotenv
import os
import pandas as pd


load_dotenv()

# Connect to the MySQL database
connection = mysql.connector.connect(
    host=os.getenv("db_host"),
    user=os.getenv("db_user"),
    password=os.getenv("db_password"),
    database=os.getenv("db_name")
)

cursor = connection.cursor()

cursor.execute("SHOW TABLES")

tables = cursor.fetchall()

print("📋 Tables in 'customer_sales_analytics' database:")
for table in tables:
    print(f" - {table[0]}")

cursor.close()
connection.close()



# create connection
load_dotenv()

def create_connection():
    try:
        # Get credentials from environment variables
        db_user = os.getenv("db_user")
        db_password = os.getenv("db_password")
        db_host = os.getenv("db_host")
        db_name = os.getenv("db_name")

        # Create the connection
        connection = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


def check_if_tables_exist():
    try:
        connection = create_connection()
        if connection is None:
            print("Connection failed. Exiting.")
            return
        
        cursor = connection.cursor()

        # Check if any tables exist in the current database
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()")
        table_count = cursor.fetchone()[0]

        if table_count > 0:
            print(f"There are {table_count} tables in the database.")
        else:
            print("No tables exist in the database.")

        cursor.close()
        connection.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

# Check if tables exist
check_if_tables_exist()




import mysql.connector

def create_connection():
    return mysql.connector.connect(
        host=os.getenv("db_host"),
        user=os.getenv("db_user"),
        password=os.getenv("db_password"),
        database=os.getenv("db_name")
    )
    

def remove_all_constraints_and_drop_tables_except_sales_data():
    try:
        connection = create_connection()
        if connection is None:
            print("Connection failed. Exiting.")
            return

        cursor = connection.cursor()

        # Disable foreign key checks
        cursor.execute("SET foreign_key_checks = 0")

        # Get all the foreign key constraints
        cursor.execute("""
        SELECT CONSTRAINT_NAME, TABLE_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE CONSTRAINT_SCHEMA = DATABASE()
        AND REFERENCED_TABLE_NAME IS NOT NULL
        """)
        constraints = cursor.fetchall()

        # Drop all foreign key constraints
        for constraint_name, table_name in constraints:
            try:
                cursor.execute(f"ALTER TABLE `{table_name}` DROP FOREIGN KEY `{constraint_name}`")
                print(f"Dropped foreign key: {constraint_name} on table: {table_name}")
            except mysql.connector.Error as err:
                print(f"Could not drop constraint {constraint_name} on {table_name}: {err}")

        # Get all table names
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        # Drop all tables except sales_data
        for (table_name,) in tables:
            if table_name != 'sales_data':
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                print(f"Dropped table: {table_name}")

        # Enable foreign key checks
        cursor.execute("SET foreign_key_checks = 1")

        connection.commit()
        cursor.close()
        connection.close()

        print("All constraints removed and tables (except sales_data) dropped successfully.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

# Run the modified function
remove_all_constraints_and_drop_tables_except_sales_data()




import mysql.connector
from tqdm import tqdm

# Connect to MySQL
connection = mysql.connector.connect( 
    host=os.getenv("db_host"),
    user=os.getenv("db_user"),
    password=os.getenv("db_password"),
    database=os.getenv("db_name")
)

cursor = connection.cursor()

# Define queries with progress tracking
tasks = [
    "Creating table: customers",
    "Creating table: shippers",
    "Creating table: categories",
    "Creating table: suppliers",
    "Creating table: orders",
    "Creating table: products",
    "Creating table: order_items",
    "Creating table: payments"
]

# Table creation queries
table_queries = [
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id VARCHAR(10) PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(100),
        phone VARCHAR(20),
        address VARCHAR(255),
        date_of_birth DATE,
        gender ENUM('Male', 'Female', 'Non-binary'),
        country VARCHAR(100),
        city VARCHAR(100),
        registration_date DATE,
        loyalty_points INT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shippers (
        shipper_name VARCHAR(100) PRIMARY KEY,
        shipping_address VARCHAR(255)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS categories (
        category_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(50) UNIQUE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS suppliers (
        supplier_id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100),
        country_of_origin VARCHAR(100),
        UNIQUE(name, country_of_origin)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id VARCHAR(10) PRIMARY KEY,
        customer_id VARCHAR(10),
        order_date DATE,
        order_status ENUM('Pending','Shipped','Delivered','Cancelled','Returned'),
        shipper_name VARCHAR(100),
        shipping_address VARCHAR(255),
        order_amount DECIMAL(10,2),
        shipping_cost DECIMAL(10,2),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY (shipper_name) REFERENCES shippers(shipper_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        product_id VARCHAR(10) PRIMARY KEY,
        product_name VARCHAR(100),
        category_id INT,
        price DECIMAL(10,2),
        stock_quantity INT,
        supplier_id INT,
        warranty_period VARCHAR(50),
        country_of_origin VARCHAR(100),
        FOREIGN KEY (category_id) REFERENCES categories(category_id),
        FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id VARCHAR(10) PRIMARY KEY,
        order_id VARCHAR(10),
        product_id VARCHAR(10),
        quantity INT,
        unit_price DECIMAL(10,2),
        discount DECIMAL(5,2),
        FOREIGN KEY (order_id) REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS payments (
        payment_id VARCHAR(10) PRIMARY KEY,
        order_id VARCHAR(10),
        payment_date DATE,
        amount DECIMAL(10,2),
        payment_method ENUM('Credit Card','PayPal','Bank Transfer','Mobile Money'),
        payment_status ENUM('Completed','Pending','Failed'),
        payment_description VARCHAR(255),
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
    )
    """
]

# Execute and show progress
for task, query in tqdm(zip(tasks, table_queries), total=len(table_queries), desc="Creating Tables"):
    cursor.execute(query)

connection.commit()

# populate the tables using SELECT DISTINCT from sales_data
insert_tasks = [
    "Inserting: customers",
    "Inserting: shippers",
    "Inserting: categories",
    "Inserting: suppliers",
    "Inserting: orders",
    "Inserting: products",
    "Inserting: order_items",
    "Inserting: payments"
]

# Step-by-step inserts with status
for step in tqdm(insert_tasks, desc="Inserting Data"):
    if step == "Inserting: customers":
        cursor.execute("SELECT DISTINCT customer_id, name, email, phone, address, date_of_birth, gender, country, city, registration_date, loyalty_points FROM sales_data")
        for row in cursor.fetchall():
            cursor.execute("""
                INSERT IGNORE INTO customers VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, row)

    elif step == "Inserting: shippers":
        cursor.execute("SELECT DISTINCT shipper_name, shipping_address FROM sales_data")
        for row in cursor.fetchall():
            cursor.execute("INSERT IGNORE INTO shippers VALUES (%s, %s)", row)

    elif step == "Inserting: categories":
        cursor.execute("SELECT DISTINCT category FROM sales_data")
        for row in cursor.fetchall():
            cursor.execute("INSERT IGNORE INTO categories (name) VALUES (%s)", (row[0],))

    elif step == "Inserting: suppliers":
        cursor.execute("SELECT DISTINCT supplier, country_of_origin FROM sales_data")
        for row in cursor.fetchall():
            cursor.execute("INSERT IGNORE INTO suppliers (name, country_of_origin) VALUES (%s, %s)", row)

    elif step == "Inserting: orders":
        cursor.execute("SELECT DISTINCT order_id, customer_id, order_date, order_status, shipper_name, shipping_address, order_amount, shipping_cost FROM sales_data")
        for row in cursor.fetchall():
            cursor.execute("INSERT IGNORE INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", row)

    elif step == "Inserting: products":
        cursor.execute("SELECT DISTINCT product_id, product_name, category, price, stock_quantity, supplier, warranty_period, country_of_origin FROM sales_data")
        for row in cursor.fetchall():
            # Get category_id
            cursor.execute("SELECT category_id FROM categories WHERE name = %s", (row[2],))
            category_id = cursor.fetchone()
            # Get supplier_id
            cursor.execute("SELECT supplier_id FROM suppliers WHERE name = %s AND country_of_origin = %s", (row[5], row[7]))
            supplier_id = cursor.fetchone()
            if category_id and supplier_id:
                cursor.execute("""
                    INSERT IGNORE INTO products (product_id, product_name, category_id, price, stock_quantity, supplier_id, warranty_period, country_of_origin)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (row[0], row[1], category_id[0], row[3], row[4], supplier_id[0], row[6], row[7]))

    elif step == "Inserting: order_items":
        cursor.execute("SELECT DISTINCT order_item_id, order_id, product_id, quantity, unit_price, discount FROM sales_data")
        for row in cursor.fetchall():
            cursor.execute("INSERT IGNORE INTO order_items VALUES (%s,%s,%s,%s,%s,%s)", row)

    elif step == "Inserting: payments":
        cursor.execute("SELECT DISTINCT payment_id, order_id, payment_date, amount, payment_method, payment_status, payment_description FROM sales_data")
        for row in cursor.fetchall():
            cursor.execute("INSERT IGNORE INTO payments VALUES (%s,%s,%s,%s,%s,%s,%s)", row)

connection.commit()
cursor.close()
connection.close()
print("✅ Normalization completed with status bar.")




import mysql.connector
import pandas as pd

# Connect to MySQL
connection = mysql.connector.connect( 
    host=os.getenv("db_host"),
    user=os.getenv("db_user"),
    password=os.getenv("db_password"),
    database=os.getenv("db_name")
)

cursor = connection.cursor()

# List of tables to export to CSV
tables = ['customers', 'shippers', 'orders', 'categories', 'suppliers', 'products', 'order_items', 'payments']

# Iterate over each table and save to CSV
for table in tables:
    # query to select all data from the table
    query = f"SELECT * FROM {table}"
    
    # execute the query
    cursor.execute(query)
    
    # fetch all rows
    data = cursor.fetchall()
    
    # fetch the column names
    column_names = [desc[0] for desc in cursor.description]
    
    # create a pandas df from the query results
    df = pd.DataFrame(data, columns=column_names)
    
    # save the df to CSV
    df.to_csv(f"{table}.csv", index=False)  # Save the table as CSV with the table name as the file
    
    print(f"Exported {table} to CSV.")

# close cursor and connection
cursor.close()
connection.close()

print("All tables have been exported to CSV successfully.")



import mysql.connector

# Establish MySQL connection
connection = mysql.connector.connect( 
    host=os.getenv("db_host"),
    user=os.getenv("db_user"),
    password=os.getenv("db_password"),
    database=os.getenv("db_name")
)

cursor = connection.cursor()

# foreign key relationships to check
foreign_keys = [
    # Table 'orders' references 'customers'
    ("orders", "customer_id", "customers", "customer_id"),
    # Table 'orders' references 'shippers'
    ("orders", "shipper_name", "shippers", "shipper_name"),
    # Table 'order_items' references 'orders'
    ("order_items", "order_id", "orders", "order_id"),
    # Table 'order_items' references 'products'
    ("order_items", "product_id", "products", "product_id"),
    # Table 'payments' references 'orders'
    ("payments", "order_id", "orders", "order_id"),
]

# check foreign key integrity
def check_foreign_key(table, foreign_column, referenced_table, primary_column):
    query = f"""
    SELECT {foreign_column} FROM {table}
    WHERE {foreign_column} NOT IN (SELECT {primary_column} FROM {referenced_table});
    """
    cursor.execute(query)
    invalid_references = cursor.fetchall()
    
    if invalid_references:
        print(f"Found invalid foreign key references in table '{table}' for column '{foreign_column}':")
        for record in invalid_references:
            print(f"Invalid reference: {record[0]}")
    else:
        print(f"All foreign key references in table '{table}' for column '{foreign_column}' are valid.")

# check all foreign key relationships
for table, foreign_column, referenced_table, primary_column in foreign_keys:
    check_foreign_key(table, foreign_column, referenced_table, primary_column)

# Close cursor and connection
cursor.close()
connection.close()

print("Foreign key integrity check completed.")