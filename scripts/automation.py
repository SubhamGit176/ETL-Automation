import requests
import csv
import schedule
import time
from datetime import datetime

# Define the API endpoint
API_URL = 'https://fakestoreapi.com/products'

# Define the CSV file paths
CSV_FILE = 'data/transformed_sales_data.csv'
LOG_FILE = 'data/source_data_log.csv'

# Function to fetch data from API
def fetch_real_time_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        new_data = response.json()
        log_source_data(new_data)  # Log the source data with timestamp
        transformed_data = transform_data(new_data)
        append_to_csv(transformed_data)
        print(f"{datetime.now()} - Data updated successfully")
    else:
        print(f"{datetime.now()} - Failed to fetch data: {response.status_code}")

# Function to log the source data with timestamp
def log_source_data(data):
    timestamp = datetime.now().isoformat()
    with open(LOG_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Check if the file is empty to write the header
        f.seek(0, 2)
        if f.tell() == 0:
            writer.writerow(['Timestamp', 'Data'])
        
        for item in data:
            writer.writerow([timestamp, item])

# Function to transform the data
def transform_data(data):
    transformed_data = []
    for item in data:
        order_date = item.get('date', '')
        price = item.get('price', 0)
        count = item.get('rating', {}).get('count', 0) if isinstance(item.get('rating'), dict) else 0
        total_sales = price * count
        month = order_date[:7]  # Assuming 'date' is in 'YYYY-MM-DD' format

        transformed_data.append([order_date, item.get('title', ''), price, count, total_sales, month])
    return transformed_data

# Function to append data to CSV
def append_to_csv(data):
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Check if the file is empty to write the header
        f.seek(0, 2)
        if f.tell() == 0:
            writer.writerow(['Order Date', 'Title', 'Price', 'Count', 'Total Sales', 'Month'])
        
        writer.writerows(data)

# Schedule the data fetching task to run every hour
#schedule.every().hour.do(fetch_real_time_data)

# Run the scheduler
#while True:
#    schedule.run_pending()
#    time.sleep(1)

fetch_real_time_data()