import csv
from collections import defaultdict

# Define the CSV file paths
TRANSFORMED_CSV_FILE = 'data/transformed_sales_data.csv'
VALIDATION_RESULTS_FILE = 'data/validation_results.csv'

# Function to read CSV data
def read_csv(file_path):
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        data = list(reader)
    return data

# Function to check for duplicates
def check_duplicates(data):
    headers = data[0]
    records = data[1:]
    seen = set()
    duplicates = []
    for row in records:
        row_tuple = tuple(row)
        if row_tuple in seen:
            duplicates.append(row)
        else:
            seen.add(row_tuple)
    return headers, duplicates

# Function to check for null values
def check_null_values(data):
    headers = data[0]
    records = data[1:]
    null_values = defaultdict(list)
    for i, row in enumerate(records):
        for j, value in enumerate(row):
            if value == '' or value is None:
                null_values[headers[j]].append(i + 1)  # +1 to account for header row
    return null_values

# Function to count the total number of records
def count_records(data):
    return len(data) - 1  # Subtract 1 to exclude header row

# Function to write validation results to CSV
def write_validation_results(duplicates, null_values, total_records):
    with open(VALIDATION_RESULTS_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write duplicate records
        writer.writerow(['Duplicate Records'])
        if duplicates:
            writer.writerow(['Order Date', 'Title', 'Price', 'Count', 'Total Sales', 'Month'])
            writer.writerows(duplicates)
        else:
            writer.writerow(['No duplicates found'])
        
        writer.writerow([])  # Empty row for separation
        
        # Write null values
        writer.writerow(['Null Values'])
        if null_values:
            writer.writerow(['Column', 'Row Numbers'])
            for column, rows in null_values.items():
                writer.writerow([column, ', '.join(map(str, rows))])
        else:
            writer.writerow(['No null values found'])
        
        writer.writerow([])  # Empty row for separation
        
        # Write total record count
        writer.writerow(['Total Records', total_records])

# Main function to perform validations and write results
def main():
    data = read_csv(TRANSFORMED_CSV_FILE)
    
    # Perform validation checks
    headers, duplicates = check_duplicates(data)
    null_values = check_null_values(data)
    total_records = count_records(data)
    
    # Write validation results to CSV
    write_validation_results(duplicates, null_values, total_records)
    print('Validation checks completed and results stored in:', VALIDATION_RESULTS_FILE)

if __name__ == "__main__":
    main()
