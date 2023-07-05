import psycopg2
import csv
import random
import asyncio
import time
import threading
import datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request, send_file

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="Loop_database",
    user="postgres",
    password="Ovd@0312"
)

# Create a cursor
cursor = conn.cursor()

complete_report= []
REPORT_TABLE = "Report"
REPORT_ID = "report_id"
REPORT_STATUS = "status"

STORE_STATUS_TABLE ="store_status"
STORE_ID = "store_id"
STORE_STATUS_TIME = "time"
STORE_STATUS = "status"

TIMEZONE_TABLE = "timezone"
STORE_TIME_ZONE = "time_zone"

BUSINESS_HOURS_TABLE = "business_hours"
WEEKDAY =  "weekday"
START_TIME = "start_time"
END_TIME= "end_time"

INTERPOLATION_INTERVAL_MINS = 45

CURRENT_TIMESTAMP = datetime.datetime.strptime("2023-01-25 23:59:59", "%Y-%m-%d %H:%M:%S")

# Flask app setup
app = Flask(__name__)

# API endpoint to trigger report generation
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    # Generate a random report ID
    report_id = str(random.randint(1000, 9999))
    
    # Perform the report generation task asynchronously (you can use a task queue or threading for this)
    # generate_report_async(report_id)
    # thread = threading.Thread(target=generate_report_async, args=(report_id))
    # thread.start()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(generate_report_async(report_id))
    loop.close()    
    # asyncio.run(generate_report_async(report_id))
    # Return the report ID as the response
    return jsonify({"report_id": report_id})

# API endpoint to get the report status or CSV
@app.route('/get_report', methods=['GET'])
def get_report():
    # Get the report ID from the request
    report_id = request.args.get('report_id')
  
    # # Check the status of the report generation (you can implement your own logic here)
    if is_report_complete(report_id):
        # Retrieve the report data from the database
        # report_data = get_report_data(report_id)
        
        # # Generate the CSV file
        # csv_filename = generate_csv(report_data)
        
        # # Return the CSV file as the response
        # return send_file(csv_filename, as_attachment=True)
        return complete_report
    else:
        # Return the running status as the response
        return "still running"
    #return "hello world"

# Function to generate the report asynchronously
async def generate_report_async(report_id):
    # Implement your report generation logic here
    # You can query the database, perform calculations, and store the report data
 
    check_query = f"SELECT * FROM business_hours WHERE store_id = 0"
    cursor.execute(check_query)
    val2 = "processing"
    insert_query = f"INSERT INTO {REPORT_TABLE} ({REPORT_ID}, {REPORT_STATUS}) VALUES ({report_id}, \'{val2}\')"
    cursor.execute(insert_query)
    conn.commit()
    get_report_data(report_id)
    # Once the report is generated, mark it as complete in the database
    mark_report_complete(report_id)

# Function to check if the report generation is complete
def is_report_complete(report_id):
    # Implement your logic to check if the report generation is complete
    # You can query the database to check the status of the report
    report_status_query = f"SELECT {REPORT_STATUS} FROM {REPORT_TABLE} WHERE {REPORT_ID} = {report_id}"
    cursor.execute(report_status_query)
    res = cursor.fetchall()
    print(res)
    
    if res[0][0] == "completed":
        return True
    return False  # Placeholder implementation

def get_uptime(intervals,start_time, end_time):
    intersected_intervals = []
    
    for interval in intervals:
        interval_start = interval[0]
        interval_end = interval[1]
        
        if interval_end >= start_time and interval_start <= end_time:
            # Interval overlaps with the specified range, add it to the result
            intersection_start = max(interval_start, start_time)
            intersection_end = min(interval_end, end_time)
            intersected_intervals.append([intersection_start, intersection_end])
    # print(intersected_intervals)
    total_uptime=0;
    for elements in intersected_intervals:
        elements_start_time = datetime.datetime.strptime(elements[0],'%Y-%m-%d %H:%M:%S')
        elements_end_time = datetime.datetime.strptime(elements[1],'%Y-%m-%d %H:%M:%S')
        total_uptime= total_uptime+(elements_end_time-elements_start_time).total_seconds()
    return total_uptime/60
        
    

def merge_intervals(intervals):
    if not intervals:
        return[]
    # intervals.sort(key=lambda x: x[0])

    merged = []
    start = intervals[0][0]
    end = intervals[0][1]
    # merged.append[start,end]
    for interval in intervals[1:]:
        if interval[0] <= end:
            # Interval overlaps with the previous one, update the end time
            # print("skip it",start, end, interval[0],interval[1])
            end = max(end, interval[1])
        else:
            # Interval doesn't overlap, add the merged interval to the result
           
            merged.append([start, end])
            start = interval[0]
            end = interval[1]
            # print("add it",start,end, intervals[0],intervals[1])

    # Add the last merged interval
    merged.append([start, end])

    return merged

# Function to retrieve the report data from the database
def get_report_data(report_id):
    # Implement your logic to retrieve the report data from the database
    # You can use SQL queries to fetch the required data based on the report ID
    # Return the report data as a list or dictionary


    #TODO: remove query limit. limit is only for fast testing
    report_gen_start_time = datetime.datetime.now()
    
    get_data = f"SELECT DISTINCT {STORE_ID} FROM {STORE_STATUS_TABLE}"
    cursor.execute(get_data)
    stores = cursor.fetchall()
    
    for store in stores:
        store_id = store[0]
        start_time = [0,0,0,0,0,0,0]
        end_time = [0,0,0,0,0,0,0]
        get_office_hours = f"SELECT {WEEKDAY}, {START_TIME}, {END_TIME} FROM {BUSINESS_HOURS_TABLE} WHERE {STORE_ID} = {store_id}"
        cursor.execute(get_office_hours)
        office_hours = cursor.fetchall()
        weekly_buisness_hours =[0,0,0,0,0,0,0]
        total_week_hours =0;
        for office_hour in office_hours:
            weekday = office_hour[0]
            start_time[weekday] = office_hour[1]
            end_time[weekday] = office_hour[2]
        for weekday in range(7):
            if start_time[weekday] == 0:
                start_time[weekday] = datetime.time(0,0,0)
            if type(end_time[weekday] ==0):
                # print(end_time[weekday])
                end_time[weekday] = datetime.time(23,59,59)
            current_date = datetime.datetime.now().date()
            start_datetime = datetime.datetime.combine(current_date, start_time[weekday])
            end_datetime = datetime.datetime.combine(current_date, end_time[weekday])    
            weekly_buisness_hours[weekday]= ((end_datetime-start_datetime).total_seconds())/3600
            total_week_hours+=weekly_buisness_hours[weekday]
            # total_week_hours=total_week_hours+ ((interval[1]-interval[0]).total_seconds())/3600

        get_store_timezone = f"SELECT {STORE_TIME_ZONE} FROM {TIMEZONE_TABLE} WHERE {STORE_ID} = {store_id}"
        cursor.execute(get_store_timezone)
        store_timezone = cursor.fetchall()
        if len(store_timezone) == 0:
            store_timezone = "America/Denver"
        else:
            # print(store_timezone[0])
            store_timezone = store_timezone[0][0]
            
        get_ping_timestamps = f"SELECT {STORE_STATUS_TIME}, {STORE_STATUS} FROM {STORE_STATUS_TABLE} WHERE {STORE_ID} = {store_id} ORDER BY {STORE_STATUS_TIME} ASC"
        cursor.execute(get_ping_timestamps)
        ping_timestamps = cursor.fetchall()
        localized_timestamps = []
        for ping_timestamp in ping_timestamps:
            localized_timestamps.append(ping_timestamp[0].astimezone(ZoneInfo(store_timezone)))
        
        active_intervals = []

        for time_stamp in localized_timestamps:
            active_start = time_stamp - datetime.timedelta( minutes = 30)
            active_end =  time_stamp + datetime.timedelta(minutes =30)
            interval = [active_start,active_end]
            active_intervals.append(interval)

        active_intervals= merge_intervals(active_intervals)
        # print(active_intervals)
        # {    
        #     # # 
        #     # print("before merging the length is", len(active_intervals))
        #     # 
        #     # print("after merging the length is", len(active_intervals))
        #     # # print(active_intervals)
        # }

        
        for interval in active_intervals:

            # print(interval[0],interval[1],type(interval[0]))
            
            interval[0] = interval[0].replace(tzinfo=None)
            interval[1] = interval[1].replace(tzinfo=None)
            interval[0]= interval[0].strftime('%Y-%m-%d %H:%M:%S')
            interval[1]= interval[1].strftime('%Y-%m-%d %H:%M:%S')
        
        # print(active_intervals)
            

        
        LOCAL_CURRENT_TIMESTAMP = CURRENT_TIMESTAMP.astimezone(ZoneInfo(store_timezone))
        todays_day=LOCAL_CURRENT_TIMESTAMP.weekday()
        LAST_HOUR_TIMESTAMP = LOCAL_CURRENT_TIMESTAMP - datetime.timedelta(minutes=60)
        # print(LOCAL_CURRENT_TIMESTAMP, type(LOCAL_CURRENT_TIMESTAMP))
        LOCAL_CURRENT_TIMESTAMP = LOCAL_CURRENT_TIMESTAMP.replace(tzinfo=None)
        LAST_HOUR_TIMESTAMP = LAST_HOUR_TIMESTAMP.replace(tzinfo=None)

        opening_time = datetime.datetime.combine(LOCAL_CURRENT_TIMESTAMP.date(),start_time[LOCAL_CURRENT_TIMESTAMP.weekday()])
        closing_time = datetime.datetime.combine(LOCAL_CURRENT_TIMESTAMP.date(),end_time[LOCAL_CURRENT_TIMESTAMP.weekday()])
        opening_time=opening_time.strftime('%Y-%m-%d %H:%M:%S')
        closing_time=closing_time.strftime('%Y-%m-%d %H:%M:%S')
        last_hour_str = LAST_HOUR_TIMESTAMP.strftime('%Y-%m-%d %H:%M:%S')
        local_current_str = LOCAL_CURRENT_TIMESTAMP.strftime('%Y-%m-%d %H:%M:%S')
        # print(last_hour_str,local_current_str)
        last_hour_uptime = get_uptime(active_intervals,last_hour_str,local_current_str)
        last_hour_downtime= 60- last_hour_uptime
        last_day_uptime = get_uptime(active_intervals,opening_time,closing_time)/60
        last_day_downtime = weekly_buisness_hours[todays_day]- last_day_uptime
        # print(todays_day)
        # print(last_day_uptime)
        # print(weekly_buisness_hours[todays_day])
        # print("downtime last day is ", weekly_buisness_hours[todays_day]-(get_uptime(active_intervals,opening_time,closing_time)/60))
        # print(opening_time, type(opening_time))
        # print(LOCAL_CURRENT_TIMESTAMP,type(LOCAL_CURRENT_TIMESTAMP))
        # print(LAST_HOUR_TIMESTAMP, type(LAST_HOUR_TIMESTAMP))
        # print(start_time[0],type(start_time[0]))
        # print(active_intervals[0][0],type(active_intervals[0][0]))
        # print(uptime_intervals)
        last_week_uptime=0
        # print(total_week_hours)
        for weekdays in range(7):
            start_hour= datetime.datetime.combine(LOCAL_CURRENT_TIMESTAMP.date(),start_time[weekdays])
            end_hour = datetime.datetime.combine(LOCAL_CURRENT_TIMESTAMP.date(),end_time[weekdays])
            start_hour = start_hour- datetime.timedelta(days=weekdays)
            end_hour = end_hour - datetime.timedelta(days=weekdays)
            start_hour =start_hour.strftime('%Y-%m-%d %H:%M:%S')
            end_hour= end_hour.strftime('%Y-%m-%d %H:%M:%S')
            last_week_uptime += (get_uptime(active_intervals,start_hour,end_hour)/60)

        last_week_downtime =total_week_hours- last_week_uptime
       
        complete_report.append([store_id,last_hour_uptime,last_day_uptime,last_week_uptime,last_hour_downtime,last_day_downtime,last_week_downtime])
        
        

    report_gen_end_time = datetime.datetime.now()
    # print(f"Report ban gaya hai. {(report_gen_end_time-report_gen_start_time).seconds} seconds lage.")    
    return complete_report

# Function to generate the CSV file
def generate_csv(report_data):
    # Define the CSV file path and filename
    csv_filename = 'report.csv'
    
    # Prepare the CSV file
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'update_last_week',
                      'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        # Write the CSV header
        writer.writeheader()
        
        # Write the report data to the CSV file
        for row in report_data:
            writer.writerow(row)
    
    return csv_filename

# Function to mark the report as complete in the database
def mark_report_complete(report_id):
    # Implement your logic to mark the report as complete in the database
    # You can update a column or set a flag to indicate the report completion
    val2 = "completed"
    update_query = f"UPDATE {REPORT_TABLE} SET {REPORT_STATUS} = \'{val2}\' WHERE {REPORT_ID} = {report_id}"
    cursor.execute(update_query)
    # Remember to commit the changes to the database
    conn.commit()

# Close the cursor and connection after serving the request
@app.teardown_appcontext
def close_connection(exception):
    # cursor.close()
    # conn.close()
    pass

# Run the Flask app
if __name__ == '__main__':
    app.run()

