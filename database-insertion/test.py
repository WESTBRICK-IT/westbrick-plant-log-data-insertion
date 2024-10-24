def put_date_in_mysql_format():
    date = "Mon, 06 Jan 2020 05:15:02 -0700"
    date = date.split()
    if(date[3] == "Jan"):
        month = "01"; 
    print(date)

put_date_in_mysql_format()