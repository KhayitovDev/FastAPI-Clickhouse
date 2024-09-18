from connections import ClickHouseConnection

DATABASE_NAME = "test"  # Your pre-existing database name

def create_database():
    client = ClickHouseConnection.get_client()
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        print(f"Database '{DATABASE_NAME}' created or already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")

def create_tables():
    client = ClickHouseConnection.get_client()
    try:
        # Switch to the specified database
        client.query(f"USE {DATABASE_NAME}")

        # Create `users` table if it does not exist
        client.query("""
            CREATE TABLE IF NOT EXISTS users
            (
                id UInt32,
                name String,
                email String,
                created_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY id;
        """)
        
        # Create `products` table if it does not exist
        client.query("""
            CREATE TABLE IF NOT EXISTS products
            (
                id UInt32,
                name String,
                price Float32,
                created_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY id;
        """)
        
        # Create `orders` table if it does not exist
        client.query("""
            CREATE TABLE IF NOT EXISTS orders
            (
                id UInt32,
                user_id UInt32,
                product_id UInt32,
                quantity UInt32,
                order_date DateTime
            ) ENGINE = MergeTree()
            ORDER BY id;
        """)
        
        # Create `reviews` table if it does not exist
        client.query("""
            CREATE TABLE IF NOT EXISTS reviews
            (
                id UInt32,
                product_id UInt32,
                user_id UInt32,
                rating UInt8,
                comment String,
                review_date DateTime
            ) ENGINE = MergeTree()
            ORDER BY id;
        """)
    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        ClickHouseConnection.close()
