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
    return f'{is_report_complete(report_id)}'
    # # Check the status of the report generation (you can implement your own logic here)
    if is_report_complete(report_id):
        # Retrieve the report data from the database
        # report_data = get_report_data(report_id)
        
        # # Generate the CSV file
        # csv_filename = generate_csv(report_data)
        
        # # Return the CSV file as the response
        # return send_file(csv_filename, as_attachment=True)
        return jsonify({"status": "Completed"})
    else:
        # Return the running status as the response
        return jsonify({"status": "Running"})
    #return "hello world"

# Function to generate the report asynchronously
async def generate_report_async(report_id):
    # Implement your report generation logic here
    # You can query the database, perform calculations, and store the report data
    conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="Loop_database",
    user="postgres",
    password="Ovd@0312"
)
    cursor = conn.cursor()
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
    # Return True if complete, False otherwise
    conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="Loop_database",
    user="postgres",
    password="Ovd@0312"
)
    cursor = conn.cursor()
    report_status_query = f"SELECT {REPORT_STATUS} FROM {REPORT_TABLE} WHERE {REPORT_ID} = {report_id}"
    cursor.execute(report_status_query)
    res = cursor.fetchall()
    return res
    if res[0][0] == "completed":
        return True
    return False  # Placeholder implementation

def get_uptime(start_time, end_time):
    pass

def merge_intervals(intervals):
    return intervals
# Function to retrieve the report data from the database
def get_report_data(report_id):
    # Implement your logic to retrieve the report data from the database
    # You can use SQL queries to fetch the required data based on the report ID
    # Return the report data as a list or dictionary


    #TODO: remove query limit. limit is only for fast testing
    report_gen_start_time = datetime.datetime.now()
    get_data = f"SELECT DISTINCT {STORE_ID} FROM {STORE_STATUS_TABLE} LIMIT 5"
    cursor.execute(get_data)
    stores = cursor.fetchall()
    
    for store in stores:
        store_id = store[0]
        start_time = [0,0,0,0,0,0,0]   #datetime.time object -> sirf time, no date
        end_time = [0,0,0,0,0,0,0]
        get_office_hours = f"SELECT {WEEKDAY}, {START_TIME}, {END_TIME} FROM {BUSINESS_HOURS_TABLE} WHERE {STORE_ID} = {store_id}"
        cursor.execute(get_office_hours)
        office_hours = cursor.fetchall()
        for office_hour in office_hours:
            weekday = office_hour[0]
            start_time[weekday] = office_hour[1]
            end_time[weekday] = office_hour[2]


        for weekday in range(7):
            if start_time[weekday] == 0:
                start_time[weekday] = datetime.time(0,0)
            if end_time[weekday] == 0:
                end_time[weekday] = datetime.time(23,59)

        get_store_timezone = f"SELECT {STORE_TIME_ZONE} FROM {TIMEZONE_TABLE} WHERE {STORE_ID} = {store_id}"
        cursor.execute(get_store_timezone)
        store_timezone = cursor.fetchall()
        if len(store_timezone) == 0:
            store_timezone = "America/Denver"
        else:
            # print(store_timezone[0])
            store_timezone = store_timezone[0][0]
            
        get_ping_timestamps = f"SELECT {STORE_STATUS_TIME}, {STORE_STATUS} FROM {STORE_STATUS_TABLE} WHERE {STORE_ID} = {store_id} ORDER BY {STORE_STATUS_TIME} DESC"
        cursor.execute(get_ping_timestamps)
        ping_timestamps = cursor.fetchall()
        localized_timestamps = []
        for ping_timestamp in ping_timestamps:
            localized_timestamps.append(ping_timestamp[0].astimezone(ZoneInfo(store_timezone)))
        
        active_intervals = []
        LOCAL_CURRENT_TIMESTAMP = CURRENT_TIMESTAMP.astimezone(ZoneInfo(store_timezone))
        day_dif = 0
        curr_timestamp = LOCAL_CURRENT_TIMESTAMP
        uptime_intervals = []
        for timestamp in localized_timestamps:
            time_dif = LOCAL_CURRENT_TIMESTAMP - timestamp
            if LOCAL_CURRENT_TIMESTAMP < timestamp:
                print(LOCAL_CURRENT_TIMESTAMP, timestamp)
                LOCAL_CURRENT_TIMESTAMP = timestamp + datetime.timedelta(minutes = 45)
            if time_dif.days > 31:
                break
            if time_dif.days > day_dif:
                active_intervals = merge_intervals(active_intervals)
                for interval in active_intervals:
                    if interval[0].time() < start_time[curr_timestamp.weekday()]:
                        interval[0] = interval[0] + (start_time[curr_timestamp.weekday()] - interval[0].time())
                    if interval[1].time() > end_time[curr_timestamp.weekday()]:
                        interval[1] = interval[1] - (interval[1].time() - end_time[curr_timestamp.weekday()])
                    if interval[0].time() > end_time[curr_timestamp.weekday()]:
                        break
                    uptime_intervals.append(interval)

                day_dif = time_dif.days 
                curr_timestamp = LOCAL_CURRENT_TIMESTAMP - datetime.timedelta(hours = 24*day_dif)
                active_intervals.clear()

            time_dif = LOCAL_CURRENT_TIMESTAMP - timestamp
            if time_dif.days == day_dif:
                interval_start = timestamp - datetime.timedelta(minutes=INTERPOLATION_INTERVAL_MINS)
                interval_end = timestamp + datetime.timedelta(minutes=INTERPOLATION_INTERVAL_MINS)
                active_intervals.append([interval_start, interval_end])
                print(interval_start, interval_end)

        if time_dif.days < 31:
            active_intervals = merge_intervals(active_intervals)
            for interval in active_intervals:
                if interval[0].time() < start_time[curr_timestamp.weekday()]:
                    interval[0] = interval[0] + (start_time[curr_timestamp.weekday()] - interval[0].time())
                if interval[1].time() > end_time[curr_timestamp.weekday()]:
                    interval[1] = interval[1] - (interval[1].time() - end_time[curr_timestamp.weekday()])
                if interval[0].time() > end_time[curr_timestamp.weekday()]:
                    break
                uptime_intervals.append(interval)

        # print(uptime_intervals)
        

    report_gen_end_time = datetime.datetime.now()
    print(f"Report ban gaya hai. {(report_gen_end_time-report_gen_start_time).seconds} seconds lage.")    
    return "Booyah"

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

