import mysql.connector
import os
from mysql.connector import Error
from datetime import datetime


# Function to extract data from the given text file

def make_sure_string_is_long_enough(words_from_line):
    if(len(words_from_line) > 1):
        return True
    else:
        return False

def get_MID(words_from_line):
    MID = words_from_line[1]    
    return MID

def get_date(words_from_line):
    i = 1
    date = ""
    while i < len(words_from_line):
        date = date + words_from_line[i]
        date = date + " "
        i = i + 1    
    return date

def get_author(words_from_line):
    i = 1
    author = ""
    while i < len(words_from_line):
        author = author + words_from_line[i]
        author = author + " "
        i = i + 1
    return author

def get_flow(words_from_line):
    flow = words_from_line[1]
    return flow

def get_inlet_temperature(words_from_line):
    inlet_temperature = words_from_line[2]
    return inlet_temperature

def get_outlet_temperature(words_from_line):
    outlet_temperature = words_from_line[2]
    return outlet_temperature

def get_pump_pressure(words_from_line):
    pump_pressure = words_from_line[2]
    return pump_pressure

def get_surge_tank_pressure(words_from_line):
    surge_tank_pressure = words_from_line[3]
    return surge_tank_pressure

def get_surge_tank_level(words_from_line):
    surge_tank_level = words_from_line[3]
    return surge_tank_level

def get_fuel_gas_pressure(words_from_line):
    fuel_gas_pressure = words_from_line[3]
    return fuel_gas_pressure

def get_data_at_end_of_array(words_from_line):
    data_at_end = words_from_line[len(words_from_line) - 1]
    return(data_at_end)

def get_data_after_name(words_from_line, length_of_name):
    i = length_of_name
    data_after_name = ""
    while i < len(words_from_line):
        data_after_name = data_after_name + words_from_line[i]
        data_after_name = data_after_name + " "
        i = i + 1
    return data_after_name

def insert_data_into_database(log_data):
    mydb = mysql.connector.connect(
        host='198.12.216.121',
        database='WestbrickPlantLogDB',
        user='cbarber',
        password='!!!Dr0w554p!!!'
    )    
    mycursor = mydb.cursor()
    sql = "INSERT INTO hot_oil2 (id, date_of_log, author, flow, inlet_temperature, outlet_temperature, pump_pressure, surge_tank_pressure, surge_tank_level, fuel_gas_pressure, stack_temperature, air_temperature, flame_condition, shift, month, day, year, date, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (log_data)    
    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")
    mycursor.close()
    mydb.close()
    print("Database connection closed.")

def day_night_to_boolean(shift):
    if(shift == "day" or shift == "Day"):
        shift = 1
    elif(shift == "night" or shift == "Night"):
        shift = 0
    return shift

def get_data_into_variable(line):     
    data = []    
    if(len(line) > 1):
        words_from_line = line.split() 
    else:
        words_from_line = line    
    if(make_sure_string_is_long_enough(words_from_line)):        
        if(words_from_line[0] == '$@MID@$:'):
            MID = get_MID(words_from_line)
            data = MID
        elif(words_from_line[0] == 'Date:'):
            date = get_date(words_from_line)   
            data = date              
        elif(words_from_line[0] == 'Author:'):
            author = get_author(words_from_line)            
            data = author
        elif(words_from_line[0] == 'Inlet:'):
            inlet = get_data_at_end_of_array(words_from_line)            
            data = inlet
        elif(words_from_line[0] == 'Sales:'):
            sales = get_data_at_end_of_array(words_from_line)
            data = sales
        elif(words_from_line[0] == 'LPG:'):
            lPG = get_data_at_end_of_array(words_from_line)            
            data = lPG
        elif(words_from_line[0] == 'Oil:'):
            oil = get_data_at_end_of_array(words_from_line)            
            data = oil
        # Have to deal with changing log, later logs have month day year        
        elif((words_from_line[0] == 'Production' and words_from_line[1] == 'Month:') or words_from_line[0] == 'Attachment:'):            
            if(words_from_line[0] == 'Attachment:'):
                print("attachment is working")
                data = ""
            else:
                month = get_data_at_end_of_array(words_from_line)            
                data = month
        elif((words_from_line[0] == 'Production' and words_from_line[1] == 'Day:') or (words_from_line[0] == 'Encoding:')):
            if(words_from_line[0] == 'Encoding:'):
                data = ""                
            else:
                day = get_data_at_end_of_array(words_from_line)            
                data = day              
        elif((words_from_line[0] == 'Production' and words_from_line[1] == 'Year:') or (words_from_line[0] == '========================================')):
            if(words_from_line[0] == '========================================'):
                data = ""
            else:
                year = get_data_at_end_of_array(words_from_line)
                data = year 
                        
        if(data == None):
            data = ""    
            
        return data

def find_number_of_lines(content):
    number_of_lines = 0
    for line in content:
        number_of_lines = number_of_lines + 1
    return number_of_lines

def find_number_of_logs(content):       
    number_of_logs = 0    
    number_of_logs = sum(1 for item in content if item.startswith('$@MID@$'))    
    print("Number of logs: " + str(number_of_logs))    
    return number_of_logs

def resize_list_to_10(log_data):
    print(log_data)

def process_file(file_path):    
    log_data = []     
    with open(file_path, 'r') as file:
        content = file.readlines() 
        number_of_lines = find_number_of_lines(content)           
        number_of_logs = find_number_of_logs(content) 
    if(number_of_logs == 0):
        print("There are no logs to insert")
        return     
    if(number_of_logs == 1):
        print("inserting 1 log")
        # fill log_data with data from content line by line and each data piece at the end of the line
        for line in content:
            log_data.append(get_data_into_variable(line))    
    
    log_data = resize_list_to_10(log_data)
    log_data = add_date_and_time(log_data)    
    # insert_data_into_database(log_data)    

def get_filename_number(filename):
    filename_number = filename[:len(filename) - 5]
    filename_number = int(filename_number)
    return filename_number

def process_folder_of_files(folder_path):      
     # Get a list of all files in the folder then insert using process file
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        print("inserting " + filename + " from " + file_path)
        filename_number = get_filename_number(filename)
        # Only add logs that have air temp
        # if(filename_number >= 220208):        
        process_file(file_path)  
    # file_path = '../database-insertion/ELOG/Pembina North/Hot Oil 2/2017/171232a.log'    
    # process_file(file_path)
def process_folder_of_folders_of_files(folder_of_folders_path):
    for folder_path in os.listdir(folder_of_folders_path):
        folder_path = os.path.join(folder_of_folders_path, folder_path)
        process_folder_of_files(folder_path)

def main():    
    folder_of_folders_path = '../database-insertion/ELOG/Pembina North/NP Daily Production 2'
    # process_folder_of_folders_of_files(folder_of_folders_path)
    process_file('../database-insertion/ELOG/Pembina North/NP Daily Production 2/2016/161005a.log')

if __name__ == "__main__":
    main()