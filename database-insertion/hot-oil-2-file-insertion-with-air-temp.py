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
    sql = "INSERT INTO hot_oil2 (id, date_of_log, author, flow, inlet_temperature, outlet_temperature, pump_pressure, surge_tank_pressure, surge_tank_level, fuel_gas_pressure, stack_temperature, air_temperature, flame_condition, shift, month, day, year, date, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (log_data)
    print(log_data)
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
    words_from_line = line.split()      
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
        elif(words_from_line[0] == 'Flow:'):
            flow = get_flow(words_from_line)            
            data = flow
        elif(words_from_line[0] == 'Inlet' and words_from_line[1] == 'Temp:'):
            inlet_temperature = get_inlet_temperature(words_from_line)        
            data = inlet_temperature    
        elif(words_from_line[0] == 'Outlet' and words_from_line[1] == 'Temp:'):
            outlet_temperature = get_outlet_temperature(words_from_line)            
            data = outlet_temperature
        elif(words_from_line[0] == 'Pump' and words_from_line[1] == 'Press:'):
            pump_pressure = get_pump_pressure(words_from_line)            
            data = pump_pressure
        elif(words_from_line[0] == 'Surge' and words_from_line[1] == 'Tank' and words_from_line[2] == 'Press:'):
            surge_tank_pressure = get_surge_tank_pressure(words_from_line)            
            data = surge_tank_pressure
        elif(words_from_line[0] == 'Surge' and words_from_line[1] == 'Tank' and words_from_line[2] == 'Level:'):
            surge_tank_level = get_surge_tank_level(words_from_line)            
            data = surge_tank_level
        elif(words_from_line[0] == 'Fuel' and words_from_line[1] == 'Gas' and words_from_line[2] == 'Press:'):
            fuel_gas_pressure = get_fuel_gas_pressure(words_from_line)            
            data = fuel_gas_pressure
        elif(words_from_line[0] == 'Stack' and words_from_line[1] == 'Temp:'):
            stack_temperature = get_data_at_end_of_array(words_from_line)            
            data = stack_temperature
        elif(words_from_line[0] == 'Air' and words_from_line[1] == 'Temp:'):
            air_temperature = get_data_at_end_of_array(words_from_line)
            data = air_temperature                        
        elif(words_from_line[0] == 'Flame' and words_from_line[1] == 'Condition:'):
            flame_condition = get_data_after_name(words_from_line, 2)            
            data = flame_condition
        elif(words_from_line[0] == 'Shift:'):
            shift = get_data_at_end_of_array(words_from_line)
            shift = day_night_to_boolean(shift)                       
            data = shift
        elif(words_from_line[0] == 'Month:'):
            month = get_data_at_end_of_array(words_from_line)            
            data = month
        elif(words_from_line[0] == 'Day:'):
            day = get_data_at_end_of_array(words_from_line)            
            data = day              
        elif(words_from_line[0] == 'Year:'):
            year = get_data_at_end_of_array(words_from_line)
            data = year 
                        
        if(data == None):
            data = ""        
        return data

def resize_list_to_17(one_log_data):
    i = len(one_log_data)
    while i > 17:
        one_log_data.pop()
        i = i - 1        
    return one_log_data

def add_date_and_time(one_log_data):    
    current_datetime = datetime.now()    
    date = current_datetime.strftime('%Y-%m-%d')    
    time = current_datetime.strftime('%H:%M:%S')    
    one_log_data.append(date)
    one_log_data.append(time)
    return one_log_data

def find_number_of_logs(content):       
    number_of_logs = 0    
    number_of_logs = sum(1 for item in content if item.startswith('$@MID@$'))    
    print("Number of logs: " + str(number_of_logs))    
    return number_of_logs

def find_number_of_lines(content):
    number_of_lines = 0
    for line in content:
        number_of_lines = number_of_lines + 1
    return number_of_lines

def get_log1(content, number_of_lines):
    log1 = []
    log1 = content.copy()
    #get index of id
    indices_of_ID = [index for index, item in enumerate(log1) if item.startswith('$@MID@$')]
    i = number_of_lines
    while i >= indices_of_ID[1]:
        log1.pop()    
        i = i - 1       
    return log1

def get_log2_of2(content, number_of_lines):
    log2 = []
    log2 = content.copy()
    #get index of id
    indices_of_ID = [index for index, item in enumerate(log2) if item.startswith('$@MID@$')]        
    i = 0
    while i < indices_of_ID[1]:
        del log2[0]
        i = i + 1        
    return log2

def get_log2_of_middle(content, number_of_lines):
    log2 = []
    log2 = content.copy()
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log2) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[1]:
        del log2[0]
        i = i + 1 
    i = number_of_lines
    while i >= indices_of_ID[2]:
        log2.pop()    
        i = i - 1
    return log2

def get_log3_of_middle(content, number_of_lines):
    log3 = []
    log3 = content.copy()
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log3) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[2]:
        del log3[0]
        i = i + 1 
    i = number_of_lines
    while i >= indices_of_ID[3]:
        log3.pop()    
        i = i - 1
    return log3

def get_log3(content, number_of_lines):
    log3 = []
    log3 = content.copy()   
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log3) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[2]:
        del log3[0]
        i = i + 1        
    return log3

def get_log4_of_middle(content, number_of_lines):
    log4 = []
    log4 = content.copy()
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log4) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[3]:
        del log4[0]
        i = i + 1 
    i = number_of_lines
    while i >= indices_of_ID[4]:
        log4.pop()    
        i = i - 1
    return log4

def get_log4(content, number_of_lines):
    log4 = []
    log4 = content.copy()   
    #get indexes of id
    indices_of_ID = [index for index, item in enumerate(log4) if item.startswith('$@MID@$')]    
    i = 0
    while i < indices_of_ID[3]:
        del log4[0]
        i = i + 1        
    return log4

def get_log5(content, number_of_lines):
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
    first_log_data = [] 
    second_log_data = []
    third_log_data = []  
    fourth_log_data = []
    fifth_log_data = [] 
    with open(file_path, 'r') as file:
        content = file.readlines() 
        number_of_lines = find_number_of_lines(content)           
        number_of_logs = find_number_of_logs(content) 
    if(number_of_logs == 0):
        print("There are no logs to insert")
        return     
    if(number_of_logs == 1):
        print("inserting 1 log")
        for line in content:
            first_log_data.append(get_data_into_variable(line))
    elif(number_of_logs == 2):
        print("inserting 2 logs")
        #split logs
        log1 = get_log1(content, number_of_lines)
        log2 = get_log2_of2(content, number_of_lines)        
        #put data into separate files
        for line in log1:
            first_log_data.append(get_data_into_variable(line))
        for line in log2:
            second_log_data.append(get_data_into_variable(line))
    elif(number_of_logs == 3):
        print("inserting 3 logs")
        #split logs
        log1 = get_log1(content, number_of_lines)
        log2 = get_log2_of_middle(content, number_of_lines)
        log3 = get_log3(content, number_of_lines)
        for line in log1:
            first_log_data.append(get_data_into_variable(line))
        for line in log2:
            second_log_data.append(get_data_into_variable(line))
        for line in log3:
            third_log_data.append(get_data_into_variable(line))
    elif(number_of_logs == 4):
        print("inserting 4 logs")
        #split logs
        log1 = get_log1(content, number_of_lines)
        log2 = get_log2_of_middle(content, number_of_lines)
        log3 = get_log3_of_middle(content, number_of_lines)
        log4 = get_log4(content, number_of_lines)        
        for line in log1:
            first_log_data.append(get_data_into_variable(line))
        for line in log2:
            second_log_data.append(get_data_into_variable(line))
        for line in log3:
            third_log_data.append(get_data_into_variable(line))
        for line in log4:
            fourth_log_data.append(get_data_into_variable(line))
    elif(number_of_logs == 5):
        print("inserting 5 logs")
        #split logs
        log1 = get_log1(content, number_of_lines)
        log2 = get_log2_of_middle(content, number_of_lines)
        log3 = get_log3_of_middle(content, number_of_lines)
        log4 = get_log4_of_middle(content, number_of_lines)  
        log5 = get_log5(content, number_of_lines)      
        for line in log1:
            first_log_data.append(get_data_into_variable(line))
        for line in log2:
            second_log_data.append(get_data_into_variable(line))
        for line in log3:
            third_log_data.append(get_data_into_variable(line))
        for line in log4:
            fourth_log_data.append(get_data_into_variable(line))
        for line in log5:
            fifth_log_data.append(get_data_into_variable(line))
    
    first_log_data = resize_list_to_17(first_log_data)
    first_log_data = add_date_and_time(first_log_data)    
    insert_data_into_database(first_log_data)
    if(number_of_logs > 1):
        second_log_data =  resize_list_to_17(second_log_data)
        second_log_data = add_date_and_time(second_log_data)        
        insert_data_into_database(second_log_data)        
    if(number_of_logs > 2):
        third_log_data =  resize_list_to_17(third_log_data)
        third_log_data = add_date_and_time(third_log_data)        
        insert_data_into_database(third_log_data)
    if(number_of_logs > 3):
        fourth_log_data =  resize_list_to_17(fourth_log_data)
        fourth_log_data = add_date_and_time(fourth_log_data)        
        insert_data_into_database(fourth_log_data)
    if(number_of_logs > 4):
        fifth_log_data =  resize_list_to_17(fifth_log_data)
        fifth_log_data = add_date_and_time(fifth_log_data)        
        insert_data_into_database(fifth_log_data)

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
        if(filename_number >= 221031):
            process_file(file_path)  
    # file_path = '../database-insertion/ELOG/Pembina North/Hot Oil 2/2017/171232a.log'    
    # process_file(file_path)
def process_folder_of_folders_of_files(folder_of_folders_path):
    for folder_path in os.listdir(folder_of_folders_path):
        folder_path = os.path.join(folder_of_folders_path, folder_path)
        process_folder_of_files(folder_path)

def main():    
    folder_of_folders_path = '../database-insertion/ELOG/Pembina North/Hot Oil 2'
    process_folder_of_folders_of_files(folder_of_folders_path)
    #process_file('../database-insertion/ELOG/Pembina North/Hot Oil 2/2022/220208a.log')

if __name__ == "__main__":
    main()