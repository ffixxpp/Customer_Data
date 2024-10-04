import pandas as pd

# Read the original CSV file
df = pd.read_csv('./mnt/data/raw_data/imed2/order_item.csv', low_memory=False)

# Split the dataframe into chunks of 10,000 rows
chunks = [df[i:i+300000] for i in range(0, len(df), 300000)]

# Save each chunk as a separate CSV file
for i, chunk in enumerate(chunks):
    chunk.to_csv(f'./mnt/data/raw_data/imed2/order_item/order_item_{i}.csv', index=False)
