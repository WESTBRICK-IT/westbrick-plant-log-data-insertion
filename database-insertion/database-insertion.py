# Written by Christopher Barber September 2024
import os
import mysql.connector
import re

# Connect to MySQL database
def connect_to_db():
    return mysql.connector.connect(
        host='your_host',
        user='your_user',
        password='your_password',
        database='your_database'
    )

# Function to extract data from the given text file
def extract_data_from_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Use regex to extract fields
    data = {}
    data['MID'] = re.search(r'\$@MID@\$\: (\d+)', content).group(1)
    data['Date'] = re.search(r'Date:\s*(.*)', content).group(1).strip()
    data['Author'] = re.search(r'Author:\s*(.*)', content).group(1).strip()
    data['Surge_Tank_Temp'] = re.search(r'Surge Tank Temp:\s*(\d+)', content).group(1)
    data['Surge_Tank_Press'] = re.search(r'Surge Tank Press:\s*(\d+)', content).group(1)
    data['Surge_Tank_Level'] = re.search(r'Surge Tank Level:\s*(\d+\.\d+)', content).group(1)
    data['Outlet_Temp'] = re.search(r'Outlet Temp:\s*(\d+)', content).group(1)
    data['Stack_Temp'] = re.search(r'Stack Temp:\s*(\d+)', content).group(1)
    data['Pump_Press'] = re.search(r'Pump Press:\s*(\d+)', content).group(1)
    data['Flame_Condition'] = re.search(r'Flame Condition:\s*(.*)', content).group(1).strip()
    data['Shift'] = re.search(r'Shift:\s*(.*)', content).group(1).strip()
    data['Month'] = re.search(r'Month:\s*(.*)', content).group(1).strip()
    data['Day'] = re.search(r'Day:\s*(\d+)', content).group(1)
    data['Year'] = re.search(r'Year:\s*(\d+)', content).group(1)

    return data

# Function to insert data into the database
def insert_data_to_db(conn, data):
    cursor = conn.cursor()
    insert_query = """
    INSERT INTO your_table_name 
    (MID, Date, Author, Surge_Tank_Temp, Surge_Tank_Press, Surge_Tank_Level, 
    Outlet_Temp, Stack_Temp, Pump_Press, Flame_Condition, Shift, Month, Day, Year) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, 
                   (data['MID'], data['Date'], data['Author'], data['Surge_Tank_Temp'], 
                    data['Surge_Tank_Press'], data['Surge_Tank_Level'], 
                    data['Outlet_Temp'], data['Stack_Temp'], data['Pump_Press'], 
                    data['Flame_Condition'], data['Shift'], data['Month'], 
                    data['Day'], data['Year']))
    conn.commit()
    cursor.close()

# Main function to process files
def process_files(data_directory):
    conn = connect_to_db()
    try:
        for root, _, files in os.walk(data_directory):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    data = extract_data_from_file(file_path)
                    insert_data_to_db(conn, data)
    finally:
        conn.close()

# Run the script on specific directory
if __name__ == "__main__":
    directory_to_process = '/path/to/your/data_directory'
    process_files(directory_to_process)