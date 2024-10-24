# Written by Christopher Barber September 2024
import mysql.connector
import os
from mysql.connector import Error
from datetime import datetime
import re


# Function to extract data from the given text file

def make_sure_string_is_long_enough(list_of_words_from_line):
    if(len(list_of_words_from_line) > 1):
        return True
    else:
        return False

def get_MID(list_of_words_from_line):
    MID = list_of_words_from_line[1]    
    return MID

# def get_date(list_of_words_from_line):
#     i = 1
#     date = ""
#     while i < len(list_of_words_from_line):
#         date = date + list_of_words_from_line[i]
#         date = date + " "
#         i = i + 1    
#     return date

# def get_author(list_of_words_from_line):
#     i = 1
#     author = ""
#     while i < len(list_of_words_from_line):
#         author = author + list_of_words_from_line[i]
#         author = author + " "
#         i = i + 1
#     return author

# def get_operator(list_of_words_from_line):
#     i = 1
#     author = ""
#     while i < len(list_of_words_from_line):
#         author = author + list_of_words_from_line[i]
#         author = author + " "
#         i = i + 1
#     return author

# def get_flow(list_of_words_from_line):
#     flow = list_of_words_from_line[1]
#     return flow

# def get_inlet_temperature(list_of_words_from_line):
#     inlet_temperature = list_of_words_from_line[2]
#     return inlet_temperature

# def get_outlet_temperature(list_of_words_from_line):
#     outlet_temperature = list_of_words_from_line[2]
#     return outlet_temperature

# def get_pump_pressure(list_of_words_from_line):
#     pump_pressure = list_of_words_from_line[2]
#     return pump_pressure

# def get_surge_tank_pressure(list_of_words_from_line):
#     surge_tank_pressure = list_of_words_from_line[3]
#     return surge_tank_pressure

# def get_surge_tank_level(list_of_words_from_line):
#     surge_tank_level = list_of_words_from_line[3]
#     return surge_tank_level

# def get_fuel_gas_pressure(list_of_words_from_line):
#     fuel_gas_pressure = list_of_words_from_line[3]
#     return fuel_gas_pressure

def get_data_at_end_of_array(list_of_words_from_line):
    data_at_end = list_of_words_from_line[len(list_of_words_from_line) - 1]
    return(data_at_end)

def get_data_after_name(list_of_words_from_line, length_of_name):
    i = length_of_name
    data_after_name = ""
    while i < len(list_of_words_from_line):
        data_after_name = data_after_name + list_of_words_from_line[i]
        data_after_name = data_after_name + " "
        i = i + 1
    return data_after_name

def insert_data_into_database(log_data):
    mydb = mysql.connector.connect(
        host='198.12.216.121',
        database='WestbrickPlantLogDB',
        user='cbarber',
        password='!!!Dr0w554p!!!',
        connection_timeout=300
    )    
    
    mycursor = mydb.cursor()
    sql = "INSERT INTO volcano_2 (id, date_of_log, author, surge_tank_temperature, surge_tank_pressure, surge_tank_level, pump_pressure, outlet_temperature, stack_temperature, flame_condition, shift, month, day, year, remark, date, time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
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

# def get_shift(list_of_words_from_line):
#     shift = get_data_at_end_of_array(list_of_words_from_line)
#     shift = day_night_to_boolean(shift)
#     return shift

# def get_data_into_variable(line):     
#     data = []    
#     if(len(line) > 1):
#         list_of_words_from_line = line.split() 
#     else:
#         list_of_words_from_line = line    
#     if(make_sure_string_is_long_enough(list_of_words_from_line)):        
#         if(list_of_words_from_line[0] == '$@MID@$:'):
#             MID = get_MID(list_of_words_from_line)
#             data = MID
#         elif(list_of_words_from_line[0] == 'Date:'):
#             date = get_date(list_of_words_from_line)   
#             data = date              
#         elif(list_of_words_from_line[0] == 'Author:'):
#             author = get_author(list_of_words_from_line)            
#             data = author
#         elif(list_of_words_from_line[0] == 'Shift:'):
#             shift = get_shift(list_of_words_from_line)
#             data = shift
#         elif(list_of_words_from_line[0] == 'Operators:'):
#             operators = get_operator(list_of_words_from_line)
#             data = operators
#         elif(list_of_words_from_line[0] == 'Shift' and list_of_words_from_line[1] == 'Handover' and list_of_words_from_line[2] == 'Meeting:'):
#         if(data == None):
#             data = ""                
#         return data

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
    while i > indices_of_ID[1]:
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
    while i >= indices_of_ID[2]:
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
    while i >= indices_of_ID[3]:
        log3.pop()    
        i = i - 1
    return log3

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
    while i >= indices_of_ID[4]:
        log4.pop()    
        i = i - 1
    return log4

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

def get_id(content):
    #Find ID by going through each line
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "$@MID@$:"):
                return line_split_up[1]
    return ""

def get_date(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Date:"):
                date = line_split_up[1:]
                date = ' '.join(date)            
                return date
    return ""

def get_author(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Author:"):
                author = line_split_up[1:]
                author = ' '.join(author)            
                return author
    return ""

def get_category(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Category:"):
                category = line_split_up[1:]
                category = ' '.join(category)            
                return category
    return ""

def get_plant_status(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Plant" and line_split_up[1] == "Status:"):
                plant_status = line_split_up[2:]
                plant_status = ' '.join(plant_status)
                return plant_status
    return ""

def get_amine_bag_filter_changed(content):    
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 4):
            if(line_split_up[0] == "Amine" and line_split_up[1] == "Bag" and line_split_up[2] == "Filter" and line_split_up[3] == "changed:"):
                anime_bag_filter_changed = line_split_up[4:]
                anime_bag_filter_changed = ' '.join(anime_bag_filter_changed)            
                return anime_bag_filter_changed
    return ""

def get_glycol_regen_filter_changed(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 4):
            if(line_split_up[0] == "Glycol" and line_split_up[1] == "Regen" and line_split_up[2] == "Filter" and line_split_up[3] == "changed:"):
                glycol_regen_filter_changed = line_split_up[4:]
                glycol_regen_filter_changed = ' '.join(glycol_regen_filter_changed)            
                return glycol_regen_filter_changed
    return ""

def remove_html(remark):
    clean = re.compile('<.*?>')
    remark = re.sub(clean, '', remark)
    remark = remark.replace('\t', '').replace('\n', '')
    remark = remark.replace('&nbsp;', '')
    remark = remark.replace("&gt;", "")
    remark = remark.replace("&amp;", "")
    return remark

def get_remark(content):    
    i = 0
    remark = ""
    start_of_remark = len(content)
    for line in content:                
        if(line.strip() == "========================================"):
            start_of_remark = i
        i = i + 1            
    i = 0    
    for line in content:
        if(i > start_of_remark):
            remark += line
        i = i + 1    
    remark = remove_html(remark)
    return remark

def get_operators(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Operators:"):
                operators = line_split_up[1:]
                operators = ' '.join(operators)
                return operators
    return ""

def get_shift(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Shift:"):
                shift = line_split_up[1:]
                shift = ' '.join(shift)                
                if(shift == ""):
                    return None
                shift = day_night_to_boolean(shift)                
                return shift
    return None

def get_shift_handover_meeting(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Shift" and line_split_up[1] == "Handover" and line_split_up[2] == "Meeting:"):
                shift_handover_meeting = line_split_up[3:]
                shift_handover_meeting = ' '.join(shift_handover_meeting)            
                return shift_handover_meeting
    return ""

def get_start_of_shift_meeting(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 4):
            if(line_split_up[0] == "Start" and line_split_up[1] == "of" and line_split_up[2] == "Shift" and line_split_up[3] == "Meeting:"):
                start_of_shift_meeting = line_split_up[4:]
                start_of_shift_meeting = ' '.join(start_of_shift_meeting)            
                return start_of_shift_meeting
    return ""

def get_equipment_outage(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Equipment" and line_split_up[1] == "Outage:"):
                equipment_outage = line_split_up[2:]
                equipment_outage = ' '.join(equipment_outage)
                return equipment_outage
    return ""

def get_filter_change(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Filter" and line_split_up[1] == "Change:"):
                filter_change = line_split_up[2:]
                filter_change = ' '.join(filter_change)
                return filter_change
    return ""

def get_pigging(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Pigging:"):
                pigging = line_split_up[1:]
                pigging = ' '.join(pigging)
                return pigging
    return ""

def get_recycle_pumps(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Recycle" and line_split_up[1] == "Pumps:"):
                filter_change = line_split_up[2:]
                filter_change = ' '.join(filter_change)
                return filter_change
    return ""

def get_production_tank_level(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Production" and line_split_up[1] == "Tank" and line_split_up[2] == "Level:"):                
                production_tank_level = line_split_up[3:]
                production_tank_level = ' '.join(production_tank_level)
                return production_tank_level
    return ""

def get_lpg_bullet_peak_level(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 4):
            if(line_split_up[0] == "LPG" and line_split_up[1] == "Bullet" and line_split_up[2] == "Peak" and line_split_up[3] == "Level:"):
                lpg_bullet_peak_level = line_split_up[4:]
                lpg_bullet_peak_level = ' '.join(lpg_bullet_peak_level)            
                return lpg_bullet_peak_level
    return ""

def get_lpg_bullet_peak_pressure(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 4):
            if(line_split_up[0] == "LPG" and line_split_up[1] == "Bullet" and line_split_up[2] == "Peak" and line_split_up[3] == "Pressure:"):
                lpg_bullet_peak_pressure = line_split_up[4:]
                lpg_bullet_peak_pressure = ' '.join(lpg_bullet_peak_pressure)  
                return lpg_bullet_peak_pressure
    return ""

def get_berm_water_samples_taken(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 4):
            if(line_split_up[0] == "Berm" and line_split_up[1] == "Water" and line_split_up[2] == "Samples" and line_split_up[3] == "Taken:"):
                berm_water_samples_taken = line_split_up[4:]
                berm_water_samples_taken = ' '.join(berm_water_samples_taken)                 
                return berm_water_samples_taken
    return ""

def get_plant_process_discussion(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Plant" and line_split_up[1] == "Process" and line_split_up[2] == "Discussion:"):                
                plant_process_discussion = line_split_up[3:]
                plant_process_discussion = ' '.join(plant_process_discussion)
                return plant_process_discussion
    return ""

def yes_no_to_boolean(variable):
    if(variable == "Yes" or variable == "yes"):
        return 1
    elif(variable == "No" or variable == "no"):
        return 0

def get_operational_targets(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Operational" and line_split_up[1] == "Targets:"):
                operational_targets = line_split_up[2:]
                operational_targets = ' '.join(operational_targets)
                return operational_targets
    return ""

def get_overrides_or_safeties_bypassed(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 4):
            if(line_split_up[0] == "Overrides" and line_split_up[1] == "or" and line_split_up[2] == "Safeties" and line_split_up[3] == "Bypassed:"):
                overrides_or_safeties_bypassed = line_split_up[4:]
                overrides_or_safeties_bypassed = ' '.join(overrides_or_safeties_bypassed)  
                return overrides_or_safeties_bypassed
    return ""

def get_upcoming_activities(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Upcoming" and line_split_up[1] == "Activities:"):
                upcoming_activities = line_split_up[2:]
                upcoming_activities = ' '.join(upcoming_activities)
                return upcoming_activities
    return ""

def get_hse_concerns(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "HSE" and line_split_up[1] == "Concerns:"):
                hse_concerns = line_split_up[2:]
                hse_concerns = ' '.join(hse_concerns)
                return hse_concerns
    return ""

def get_regulatory_requirements(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Regulatory" and line_split_up[1] == "Requirements:"):
                regulatory_requirements = line_split_up[2:]
                regulatory_requirements = ' '.join(regulatory_requirements)
                return regulatory_requirements
    return ""

def get_staff_discussion(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Staff" and line_split_up[1] == "Discussion:"):
                staff_discussion = line_split_up[2:]
                staff_discussion = ' '.join(staff_discussion)
                return staff_discussion
    return ""

def get_weather_and_effects_on_operations(content):    
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 5):
            if(line_split_up[0] == "Weather" and line_split_up[1] == "&" and line_split_up[2] == "Effects" and line_split_up[3] == "on" and line_split_up[4] == "Operations:"):
                weather_and_effects_on_operations = line_split_up[5:]
                weather_and_effects_on_operations = ' '.join(weather_and_effects_on_operations)  
                return weather_and_effects_on_operations
    return ""

def get_permit_extensions_critical_tasks(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Permit" and line_split_up[1] == "Extensions/Critical" and line_split_up[2] == "Tasks:"):
                permit_extensions_critical_tasks = line_split_up[3:]
                permit_extensions_critical_tasks = ' '.join(permit_extensions_critical_tasks)
                return permit_extensions_critical_tasks
    return ""

def get_roustabout_utilization(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Roustabout" and line_split_up[1] == "Utilization:"):
                roustabout_utilization = line_split_up[2:]
                roustabout_utilization = ' '.join(roustabout_utilization)
                return roustabout_utilization
    return ""

def get_date_time_of_bypass(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 5):
            if(line_split_up[0] == "Date" and line_split_up[1] == "/" and line_split_up[2] == "Time" and line_split_up[3] == "of" and line_split_up[4] == "Bypass:"):
                date_time_of_bypass = line_split_up[5:]
                date_time_of_bypass = ' '.join(date_time_of_bypass)  
                return date_time_of_bypass
    return ""

def get_equipment_name(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Equipment" and line_split_up[1] == "Name:"):
                equipment_name = line_split_up[2:]
                equipment_name = ' '.join(equipment_name)
                return equipment_name
    return ""

def get_device_tag(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Device" and line_split_up[1] == "Tag:"):
                device_tag = line_split_up[2:]
                device_tag = ' '.join(device_tag)
                return device_tag
    return ""

def get_estimated_bypass_renewal(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Estimated" and line_split_up[1] == "Bypass" and line_split_up[2] == "Renewal:"):
                estimated_bypass_renewal = line_split_up[3:]
                estimated_bypass_renewal = ' '.join(estimated_bypass_renewal)
                return estimated_bypass_renewal
    return ""

def get_safe_work_permit_number(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 4):
            if(line_split_up[0] == "Safe" and line_split_up[1] == "Work" and line_split_up[2] == "Permit" and line_split_up[3] == "#:"):
                safe_work_permit_number = line_split_up[4:]
                safe_work_permit_number = ' '.join(safe_work_permit_number) 
                #print("Safe Work Permit Number: " + type(safe_work_permit_number))
                if(safe_work_permit_number == ""):
                    return None                
                return safe_work_permit_number
    return None

def get_estimated_bypass_removal_date_time(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 6):
            if(line_split_up[0] == "Estimated" and line_split_up[1] == "Bypass" and line_split_up[2] == "Removal" and line_split_up[3] == "Date" and line_split_up[4] == "/" and line_split_up[5] == "Time:"):
                estimated_bypass_removal_date_time = line_split_up[6:]
                estimated_bypass_removal_date_time = ' '.join(estimated_bypass_removal_date_time)  
                return estimated_bypass_removal_date_time
    return ""

def get_date_time_of_bypass_removal(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 6):
            if(line_split_up[0] == "Date" and line_split_up[1] == "/" and line_split_up[2] == "Time" and line_split_up[3] == "of" and line_split_up[4] == "Bypass" and line_split_up[5] == "Removal:"):
                date_time_of_bypass_removal = line_split_up[6:]
                date_time_of_bypass_removal = ' '.join(date_time_of_bypass_removal)  
                return date_time_of_bypass_removal
    return ""

def get_bypass_form_number(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Bypass" and line_split_up[1] == "Form" and line_split_up[2] == "#:"):
                bypass_form_number = line_split_up[3:]
                bypass_form_number = ' '.join(bypass_form_number)
                return bypass_form_number
    return ""

def get_pressure(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Pressure:"):
                pressure = line_split_up[1:]
                pressure = ' '.join(pressure)
                if(pressure == ""):
                    return None
                return pressure
    return None

def get_temperature(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Temperature:"):
                temperature = line_split_up[1:]
                temperature = ' '.join(temperature)
                if(temperature == ""):
                    return None
                return temperature
    return None

def get_front_water_level(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Front" and line_split_up[1] == "Water" and line_split_up[2] == "Lvl:"):
                front_water_level = line_split_up[3:]
                front_water_level = ' '.join(front_water_level)
                if(front_water_level == ""):
                    return None
                return front_water_level
    return None

def get_back_water_level(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Back" and line_split_up[1] == "Water" and line_split_up[2] == "Lvl:"):
                back_water_level = line_split_up[3:]
                back_water_level = ' '.join(back_water_level)
                if(back_water_level == ""):
                    return None
                return back_water_level
    return None

def get_flame_condition(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Flame" and line_split_up[1] == "Condition:"):
                flame_condition = line_split_up[2:]
                flame_condition = ' '.join(flame_condition)
                return flame_condition
    return ""

def get_month(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Month:"):
                month = line_split_up[1:]
                month = ' '.join(month)                
                return month
    return ""

def get_day(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Day:"):
                day = line_split_up[1:]
                day = ' '.join(day)                
                return day
    return ""

def get_year(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 1):
            if(line_split_up[0] == "Year:"):
                year = line_split_up[1:]
                year = ' '.join(year)
                return year
    return ""

def get_surge_tank_temperature(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Surge" and line_split_up[1] == "Tank" and line_split_up[2] == "Temp:"):
                surge_tank_temperature = line_split_up[3:]
                surge_tank_temperature = ' '.join(surge_tank_temperature)
                if(surge_tank_temperature == ""):
                    return None
                return surge_tank_temperature
    return None

def get_surge_tank_pressure(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Surge" and line_split_up[1] == "Tank" and line_split_up[2] == "Press:"):
                surge_tank_pressure = line_split_up[3:]
                surge_tank_pressure = ' '.join(surge_tank_pressure)
                if(surge_tank_pressure == ""):
                    return None
                return surge_tank_pressure
    return None

def get_surge_tank_level(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 3):
            if(line_split_up[0] == "Surge" and line_split_up[1] == "Tank" and line_split_up[2] == "Level:"):
                surge_tank_level = line_split_up[3:]
                surge_tank_level = ' '.join(surge_tank_level)
                if(surge_tank_level == ""):
                    return None
                return surge_tank_level
    return None

def get_pump_pressure(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Pump" and line_split_up[1] == "Press:"):
                pump_pressure = line_split_up[2:]
                pump_pressure = ' '.join(pump_pressure)
                if(pump_pressure == ""):
                    return None
                return pump_pressure
    return None

def get_outlet_temperature(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Outlet" and line_split_up[1] == "Temp:"):
                outlet_temperature = line_split_up[2:]
                outlet_temperature = ' '.join(outlet_temperature)
                if(outlet_temperature == ""):
                    return None
                return outlet_temperature
    return None

def get_stack_temperature(content):
    for line in content:
        line_split_up = line.split()
        if(len(line_split_up) > 2):
            if(line_split_up[0] == "Stack" and line_split_up[1] == "Temp:"):
                stack_temperature = line_split_up[2:]
                stack_temperature = ' '.join(stack_temperature)
                if(stack_temperature == ""):
                    return None
                return stack_temperature
    return None

def get_log_data(content):
    # get each data piece individually if there is no data return blank string        
    log_data = []
    # get ID    
    ID = get_id(content)
    ID = int(ID)
    log_data.append(ID)
    print("ID: " + str(ID))
    # get Date    
    date = get_date(content)
    date= typecast_date_to_date_type(date)
    log_data.append(date)
    print("Date: " + str(date))

    # get Author
    author = get_author(content)
    log_data.append(author)
    print("Author: " + str(author))
    #get Surge Tank Temperature
    surge_tank_temperature = get_surge_tank_temperature(content)
    log_data.append(surge_tank_temperature)
    print("Surge Tank Temperature: " + str(surge_tank_temperature))
    #get Surge Tank Pressure
    surge_tank_pressure = get_surge_tank_pressure(content)
    log_data.append(surge_tank_pressure)
    print("Surge Tank Pressure: " + str(surge_tank_pressure))
    #get Surge Tank Level
    surge_tank_level = get_surge_tank_level(content)
    log_data.append(surge_tank_level)
    print("Surge Tank Level: " + str(surge_tank_level))
    #get Pump Pressure
    pump_pressure = get_pump_pressure(content)
    log_data.append(pump_pressure)
    print("Pump Pressure: " + str(pump_pressure))
    #get Outlet Temperature
    outlet_temperature = get_outlet_temperature(content)
    log_data.append(outlet_temperature)
    print("Outlet Temperature: " + str(outlet_temperature))
    #get Stack Temperature
    stack_temperature = get_stack_temperature(content)
    log_data.append(stack_temperature)
    print("Stack Temperature: " + str(stack_temperature))    
    #get Flame Condition
    flame_condition = get_flame_condition(content)
    log_data.append(flame_condition)
    print("Flame Condition: " + str(flame_condition))
    #get Shift
    shift = get_shift(content)
    log_data.append(shift)
    print("Shift: " + str(shift))
    #get Month
    month = get_month(content)
    log_data.append(month)
    print("Month: " + str(month))
    #get Day
    day = get_day(content)
    log_data.append(day)
    print("Day: " + str(day))
    #get Year
    year = get_year(content)
    log_data.append(year)
    print("Year: " + str(year))
    #get Remark
    remark = get_remark(content)
    log_data.append(remark)
    print("Remark: " + str(remark))

    print(log_data)    
    return log_data

def process_file(file_path):    
    log_data = [] 
    log_data1 = []    
    log_data2 = []
    log_data3 = []
    log_data4 = []
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
        log_data1 = get_log_data(content)  
    elif(number_of_logs == 2):
        print("Inserting 2 logs")
        # split logs
        log_data1 = get_first_log_from_content(content, number_of_lines)
        log_data2 = get_second_log_from_content_of2(content, number_of_lines)        
        log_data1 = get_log_data(log_data1)
        log_data2 = get_log_data(log_data2)
    elif(number_of_logs == 3):
        print("Inserting 3 logs")
        log_data1 = get_first_log_from_content(content, number_of_lines)
        log_data2 = get_second_log_from_content_of_middle(content, number_of_lines)        
        log_data3 = get_third_log_from_content_of3(content, number_of_lines)
        log_data1 = get_log_data(log_data1)
        log_data2 = get_log_data(log_data2)
        log_data3 = get_log_data(log_data3)
    elif(number_of_logs == 4):
        print("Inserting 4 logs")
        log_data1 = get_first_log_from_content(content, number_of_lines)
        log_data2 = get_second_log_from_content_of_middle(content, number_of_lines)        
        log_data3 = get_third_log_from_content_of_middle(content, number_of_lines)
        log_data4 = get_fourth_log_from_content_of4(content, number_of_lines)
        log_data1 = get_log_data(log_data1)
        log_data2 = get_log_data(log_data2)
        log_data3 = get_log_data(log_data3)
        log_data4 = get_log_data(log_data4)
    elif(number_of_logs == 5):
        print("Inserting 5 logs")
        log_data1 = get_first_log_from_content(content, number_of_lines)
        log_data2 = get_second_log_from_content_of_middle(content, number_of_lines)        
        log_data3 = get_third_log_from_content_of_middle(content, number_of_lines)
        log_data4 = get_fourth_log_from_content_of_middle(content, number_of_lines)
        log_data5 = get_fifth_log_from_content_of5(content, number_of_lines)
        log_data1 = get_log_data(log_data1)
        log_data2 = get_log_data(log_data2)
        log_data3 = get_log_data(log_data3)
        log_data4 = get_log_data(log_data4)
        log_data5 = get_log_data(log_data5)
    
    #We have the log data now insert it into the database
    log_data1 = add_date_and_time(log_data1)
    insert_data_into_database(log_data1)    
    if(number_of_logs > 1):    
        log_data2 = add_date_and_time(log_data2)
        insert_data_into_database(log_data2)    
    if(number_of_logs > 2):    
        log_data3 = add_date_and_time(log_data3)    
        insert_data_into_database(log_data3) 
    if(number_of_logs > 3):    
        log_data4 = add_date_and_time(log_data4)    
        insert_data_into_database(log_data4)
    if(number_of_logs > 4):    
        log_data5 = add_date_and_time(log_data5)    
        insert_data_into_database(log_data5)

def get_filename_number(filename):
    filename_number = filename[:len(filename) - 5]
    try:
        filename_number = int(filename_number)
    except ValueError:
        print("this file name is not a number dude")
        return 0
    return filename_number

def process_folder_of_files(folder_path):      
     # Get a list of all files in the folder then insert using process file
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        print("inserting " + filename + " from " + file_path)
        
        filename_number = get_filename_number(filename)        
        if(filename_number >= 220601):
            process_file(file_path)  
    # file_path = '../database-insertion/ELOG/Pembina North/Hot Oil 2/2017/171232a.log'    
    # process_file(file_path)
def process_folder_of_folders_of_files(folder_of_folders_path):
    for folder_path in os.listdir(folder_of_folders_path):
        folder_path = os.path.join(folder_of_folders_path, folder_path)
        process_folder_of_files(folder_path)

def main():        
    folder_of_folders_path = '../database-insertion/ELOG/Pembina North/Volcano 2'
    process_folder_of_folders_of_files(folder_of_folders_path)
    #process_file('../database-insertion/ELOG/Pembina North/Volcano 2/2017/170814a.log')

if __name__ == "__main__":
    main()