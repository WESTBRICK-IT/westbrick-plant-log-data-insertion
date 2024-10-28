# Written by Christopher Barber September 2024
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
    sql = "INSERT INTO np_daily_production2 (id, date_of_log, author, inlet, sales, lpg, oil, production_month, production_day, production_year, date, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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
    i = len(log_data)
    while(i > 10):            
        log_data.pop()
        i = i - 1    
    return log_data

def change_null_items_to_string(log_data):
    i = 0    
    while(i < len(log_data)):
        if(log_data[i] == None):
            log_data[i] = ""
        i = i + 1    
    return log_data

def add_date_and_time(log_data):       
    current_datetime = datetime.now()    
    date = current_datetime.strftime('%Y-%m-%d')    
    time = current_datetime.strftime('%H:%M:%S')    
    log_data.append(date)
    log_data.append(time)
    return log_data

def get_first_log_from_content(content, number_of_lines):
    log1 = []
    log1 = content.copy()
    #get index of id
    indices_of_ID = [index for index, item in enumerate(log1) if item.startswith('$@MID@$')]
    i = number_of_lines
    while i >= indices_of_ID[1]:
        log1.pop()    
        i = i - 1       
    return log1 

def get_second_log_from_content_of2(content, number_of_lines):
    log2 = []
    log2 = content.copy()
    #get index of id
    indices_of_ID = [index for index, item in enumerate(log2) if item.startswith('$@MID@$')]        
    i = 0
    while i < indices_of_ID[1]:
        del log2[0]
        i = i + 1        
    return log2

def get_second_log_from_content_of_middle(content, number_of_lines):
    log2 = []
    log2 = content.copy()
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log2) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[1]:
        del log2[0]
        i = i + 1 
    i = number_of_lines
    while i > indices_of_ID[2]:
        log2.pop()    
        i = i - 1
    return log2

def get_third_log_from_content_of3(content, number_of_lines):
    log3 = []
    log3 = content.copy()   
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log3) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[2]:
        del log3[0]
        i = i + 1        
    return log3

def get_third_log_from_content_of_middle(content, number_of_lines):
    log3 = []
    log3 = content.copy()
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log3) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[2]:
        del log3[0]
        i = i + 1 
    i = number_of_lines
    while i > indices_of_ID[3]:
        log3.pop()    
        i = i - 1
    return log3

def get_fourth_log_from_content_of4(content, number_of_lines):
    log4 = []
    log4 = content.copy()   
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log4) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[3]:
        del log4[0]
        i = i + 1        
    return log4

def get_fourth_log_from_content_of_middle(content, number_of_lines):
    log4 = []
    log4 = content.copy()
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log4) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[3]:
        del log4[0]
        i = i + 1 
    i = number_of_lines
    while i > indices_of_ID[4]:
        log4.pop()    
        i = i - 1
    return log4

def get_fifth_log_from_content_of5(content, number_of_lines):
    log5 = []
    log5 = content.copy()   
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log5) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[4]:
        del log5[0]
        i = i + 1        
    return log5


def process_file(file_path):    
    log_data = []     
    log_data2 = []
    log_data3 = []
    log_data4 = []
    log_data5 = []
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
    if(number_of_logs == 2):
        print("inserting 2 logs")
        log1 = get_first_log_from_content(content, number_of_lines)
        log2 = get_second_log_from_content_of2(content, number_of_lines)
        for line in log1:
            log_data.append(get_data_into_variable(line))
        for line in log2:
            log_data2.append(get_data_into_variable(line))    
    if(number_of_logs == 3):
        print("inserting 3 logs")
        log1 = get_first_log_from_content(content, number_of_lines)
        log2 = get_second_log_from_content_of_middle(content, number_of_lines)
        log3 = get_third_log_from_content_of3(content, number_of_lines)
        for line in log1:
            log_data.append(get_data_into_variable(line))
        for line in log2:
            log_data2.append(get_data_into_variable(line))
        for line in log3:
            log_data3.append(get_data_into_variable(line))
    if(number_of_logs == 4):
        print("inserting 4 logs")
        log1 = get_first_log_from_content(content, number_of_lines)
        log2 = get_second_log_from_content_of_middle(content, number_of_lines)
        log3 = get_third_log_from_content_of_middle(content, number_of_lines)
        log4 = get_fourth_log_from_content_of4(content, number_of_lines)
        for line in log1:
            log_data.append(get_data_into_variable(line))
        for line in log2:
            log_data2.append(get_data_into_variable(line))
        for line in log3:
            log_data3.append(get_data_into_variable(line))
        for line in log4:
            log_data4.append(get_data_into_variable(line))
    if(number_of_logs == 5):
        print("inserting 5 logs")
        log1 = get_first_log_from_content(content, number_of_lines)
        log2 = get_second_log_from_content_of_middle(content, number_of_lines)
        log3 = get_third_log_from_content_of_middle(content, number_of_lines)
        log4 = get_fourth_log_from_content_of_middle(content, number_of_lines)
        log5 = get_fifth_log_from_content_of5(content, number_of_lines)
        for line in log1:
            log_data.append(get_data_into_variable(line))
        for line in log2:
            log_data2.append(get_data_into_variable(line))
        for line in log3:
            log_data3.append(get_data_into_variable(line))
        for line in log4:
            log_data4.append(get_data_into_variable(line))
        for line in log5:
            log_data5.append(get_data_into_variable(line))

    
    log_data = resize_list_to_10(log_data)
    log_data = change_null_items_to_string(log_data)    
    log_data = add_date_and_time(log_data)    
    insert_data_into_database(log_data)    
    if(number_of_logs > 1):
        log_data2 = resize_list_to_10(log_data2)
        log_data2 = change_null_items_to_string(log_data2)    
        log_data2 = add_date_and_time(log_data2)
        insert_data_into_database(log_data2)    
    if(number_of_logs > 2):
        log_data3 = resize_list_to_10(log_data3)
        log_data3 = change_null_items_to_string(log_data3)    
        log_data3 = add_date_and_time(log_data3)    
        insert_data_into_database(log_data3) 
    if(number_of_logs > 3):
        log_data4 = resize_list_to_10(log_data4)
        log_data4 = change_null_items_to_string(log_data4)    
        log_data4 = add_date_and_time(log_data4)    
        insert_data_into_database(log_data4)
    if(number_of_logs > 4):
        log_data5 = resize_list_to_10(log_data5)
        log_data5 = change_null_items_to_string(log_data5)    
        log_data5 = add_date_and_time(log_data5)    
        insert_data_into_database(log_data5)    

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
        if(filename_number >= 240206):        
            process_file(file_path)  
    # file_path = '../database-insertion/ELOG/Pembina North/Hot Oil 2/2017/171232a.log'    
    # process_file(file_path)
def process_folder_of_folders_of_files(folder_of_folders_path):
    for folder_path in os.listdir(folder_of_folders_path):
        folder_path = os.path.join(folder_of_folders_path, folder_path)
        process_folder_of_files(folder_path)

def main():    
    folder_of_folders_path = '../database-insertion/ELOG/Pembina North/NP Daily Production 2'
    process_folder_of_folders_of_files(folder_of_folders_path)
    # process_file('../database-insertion/ELOG/Pembina North/NP Daily Production 2/2022/220812a.log')

if __name__ == "__main__":
    main()