import csv
import random
import time
import os

OUTPUT_DIR = "./jobs"
FILENAME = os.path.join(OUTPUT_DIR, "sales_heavy.csv")
TARGET_SIZE_MB = 550
BATCH_SIZE = 50000

CATEGORIES = [
    "furniture",
    "baby products",
    "electronics",
    "books",
    "clothing",
    "garden",
    "sports",
    "toys",
    "automotive",
    "health & beauty",
    "office equipment"
]

def generate_data():
    print(f"Starting generation of ~{TARGET_SIZE_MB} MB file: {FILENAME}")
    start_time = time.time()

    os.makedirs(os.path.dirname(FILENAME), exist_ok=True)

    transaction_id = 0
    current_size_mb = 0

    with open(FILENAME, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['transaction_id', 'product_id', 'category', 'price', 'quantity'])

        while current_size_mb < TARGET_SIZE_MB:
            batch = []

            # Генерация пачки данных в памяти
            for _ in range(BATCH_SIZE):
                t_id = transaction_id
                p_id = random.randint(1, 5000)
                cat = random.choice(CATEGORIES)
                price = round(random.uniform(5.0, 3000.0), random.choice([2, 3]))
                qty = random.randint(1, 20)

                batch.append([t_id, p_id, cat, price, qty])
                transaction_id += 1

            writer.writerows(batch)


            if transaction_id % (BATCH_SIZE * 5) == 0:
                f.flush()
                size_bytes = os.path.getsize(FILENAME)
                current_size_mb = size_bytes / (1024 * 1024)
                print(f"Progress: {current_size_mb:.2f} MB / {TARGET_SIZE_MB} MB ({transaction_id} rows)...")

    end_time = time.time()
    print("=" * 40)
    print(f"Done! File generated at: {FILENAME}")
    print(f"Total Rows: {transaction_id}")
    print(f"Total Size: {os.path.getsize(FILENAME) / (1024*1024):.2f} MB")
    print(f"Time taken: {end_time - start_time:.2f} sec")
    print("=" * 40)

if __name__ == "__main__":
    generate_data()
