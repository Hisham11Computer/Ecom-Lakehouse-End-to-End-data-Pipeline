import os
import random

from sqlalchemy import create_engine, text


DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

PRODUCTS = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "USB Cable"]


def generate_data(batch_size=25):
    try:
        with engine.begin() as conn:
           
            result = conn.execute(text("SELECT customer_id FROM customers;"))
            valid_customer_ids = [row[0] for row in result.fetchall()]

            if not valid_customer_ids:
                print("No customers found in the customers table!")
                return

            insert_query = text("""
                INSERT INTO orders (customer_id, product_name, price, quantity)
                VALUES (:customer_id, :product_name, :price, :quantity)
            """)

           
            batch_data = [
                {
                    "customer_id": random.choice(valid_customer_ids),
                    "product_name": random.choice(PRODUCTS),
                    "price": round(random.uniform(10.0, 500.0), 2),
                    "quantity": random.randint(1, 5),
                }
                for _ in range(batch_size)
            ]

            conn.execute(insert_query, batch_data)
            print(f"Successfully inserted {batch_size} new records into 'orders'")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise


if __name__ == "__main__":
    generate_data(batch_size=25)
