import pandas as pd
import sqlite3

# Load the spreadsheets
df0 = pd.read_csv('shipping_data_0.csv')
df1 = pd.read_csv('shipping_data_1.csv')
df2 = pd.read_csv('shipping_data_2.csv')

# Connect to the SQLite database
conn = sqlite3.connect('shipping_database.db')
cursor = conn.cursor()

# Create the table for shipping data (if it doesn't exist)
cursor.execute('''
CREATE TABLE IF NOT EXISTS shipping_data (
    origin_warehouse TEXT,
    destination_store TEXT,
    product TEXT,
    on_time BOOLEAN,
    product_quantity INTEGER,
    driver_identifier TEXT
)
''')

# Insert data from shipping_data_0.csv
for index, row in df0.iterrows():
    cursor.execute('''
        INSERT INTO shipping_data (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (row['origin_warehouse'], row['destination_store'], row['product'], row['on_time'], row['product_quantity'], row['driver_identifier']))

# Merge shipping_data_1.csv and shipping_data_2.csv on shipment_identifier
merged_df = pd.merge(df1, df2, on='shipment_identifier')

# Group by shipment_identifier to calculate product quantities
grouped_df = merged_df.groupby(['shipment_identifier', 'origin_warehouse', 'destination_store', 'driver_identifier', 'product', 'on_time']).size().reset_index(name='product_quantity')

# Insert the merged and grouped data into the database
for index, row in grouped_df.iterrows():
    cursor.execute('''
        INSERT INTO shipping_data (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (row['origin_warehouse'], row['destination_store'], row['product'], row['on_time'], row['product_quantity'], row['driver_identifier']))

# Commit the transaction and close the connection
conn.commit()
conn.close()

print("Data has been successfully inserted into the database.")