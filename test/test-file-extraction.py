import mysql.connector
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

def insert_data_into_database(page_data):
    mydb = mysql.connector.connect(
        host='198.12.216.121',
        database='WestbrickPlantLogDB',
        user='cbarber',
        password='!!!Dr0w554p!!!'
    )    
    mycursor = mydb.cursor()
    sql = "INSERT INTO hot_oil2 (id, date_of_log, author, flow, inlet_temperature, outlet_temperature, pump_pressure, surge_tank_pressure, surge_tank_level, fuel_gas_pressure, stack_temperature, flame_condition, shift, month, day, year, date, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (page_data)
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
        return data

def resize_list_to_16(one_page_data):
    i = len(one_page_data)
    while i > 16:
        one_page_data.pop()
        i = i - 1        
    return one_page_data

def add_date_and_time(one_page_data):    
    current_datetime = datetime.now()    
    date = current_datetime.strftime('%Y-%m-%d')    
    time = current_datetime.strftime('%H:%M:%S')    
    one_page_data.append(date)
    one_page_data.append(time)
    return one_page_data

def find_number_of_pages(number_of_lines):       
    if(number_of_lines < 22):
        number_of_pages = 1
    elif(number_of_lines < 43):
        number_of_pages = 2
    elif(number_of_lines < 66):
        number_of_pages = 3
    return number_of_pages

def find_number_of_lines(content):
    number_of_lines = 0
    for line in content:
        number_of_lines = number_of_lines + 1
    return number_of_lines

def get_page1(content, number_of_lines):
    page1 = []
    page1 = content.copy()
    half_way_point = number_of_lines / 2        
    i = 0
    while i < half_way_point:
        page1.pop()    
        i = i + 1       
    return page1

def get_page2(content, number_of_lines):
    page2 = []
    page2 = content.copy()
    half_way_point = number_of_lines / 2            
    i = 0
    while i < half_way_point:
        del page2[0]
        i = i + 1        
    return page2    

def process_file(file_path):    
    first_page_data = [] 
    second_page_data = []
    third_page_data = []   
    with open(file_path, 'r') as file:
        content = file.readlines()         
        number_of_lines = find_number_of_lines(content)           
        number_of_pages = find_number_of_pages(number_of_lines)        
    if(number_of_pages == 1):
        for line in content:
            first_page_data.append(get_data_into_variable(line))
    elif(number_of_pages == 2):
        #split pages
        page1 = get_page1(content, number_of_lines)
        page2 = get_page2(content, number_of_lines)
        #put data into separate files
        for line in page1:
            first_page_data.append(get_data_into_variable(line))
        for line in page2:
            second_page_data.append(get_data_into_variable(line))
    elif(number_of_pages == 3):
        #split pages
        page1 = get_page1(content, number_of_lines)
        page2 = get_page2(content, number_of_lines)
        page3 = get_page3(content, number_of_lines)
        for line in page1:
            first_page_data.append(get_data_into_variable(line))
        for line in page2:
            second_page_data.append(get_data_into_variable(line))
        for line in page3:
            third_page_data.append(get_data_into_variable(line))
    
    first_page_data = resize_list_to_16(first_page_data)
    first_page_data = add_date_and_time(first_page_data)    
    insert_data_into_database(first_page_data)
    if(number_of_pages > 1):
        second_page_data =  resize_list_to_16(second_page_data)
        second_page_data = add_date_and_time(second_page_data)
        insert_data_into_database(second_page_data)        
    if(number_of_pages > 2):
        third_page_data =  resize_list_to_16(third_page_data)
        third_page_data = add_date_and_time(third_page_data)
        insert_data_into_database(third_page_data)

def main():
    # file_path = '../database-insertion/ELOG/Pembina North/Hot Oil 2/2017/170813a.log'
    file_path = '../database-insertion/ELOG/Pembina North/Hot Oil 2/2017/171230a.log'    
    process_file(file_path)

if __name__ == "__main__":
    main()