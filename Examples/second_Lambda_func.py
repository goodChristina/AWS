# This Lambda function retrieves interconnector power flow data from DynamoDB and formats it for frontend display
# Main components:
# 1. Time handling: Converts between different time formats (datetime strings, epoch time)
# 2. DynamoDB interaction: Fetches 24 hours of power flow data
# 3. API Response: Returns formatted JSON with CORS headers enabled

# Key functions:
# - hour_rounder(): Rounds time to nearest hour for consistent data points
# - get_date_keys(): Generates 15-minute interval timestamps for last 24 hours
# - convert_to_epoch(): Converts datetime strings to epoch time for JavaScript frontend
# - get_todays_data_from_db(): Main query function to fetch and sort DynamoDB data
# - lambda_handler(): Entrypoint that handles the API request/response cycle

# Error handling:
# - Custom JSON encoder for Decimal types (DynamoDB specific)
# - Try/except blocks with detailed error messages
# - Returns appropriate HTTP status codes (200 for success, 500 for errors)

# The end result is a REST API endpoint that serves time-series power flow data
# to a frontend application, with data points every 15 minutes for the past 24 hours

# Import required libraries for time handling, AWS SDK, and data processing
import datetime
import boto3
from datetime import date
import calendar
import json
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
   # Custom encoder to convert Decimal types to float 
   # (needed because DynamoDB uses Decimal type for numbers)
   def default(self, obj):
       if isinstance(obj, Decimal):
           return float(obj)
       return super(DecimalEncoder, self).default(obj)

# Initialize DynamoDB client and reference our table
boto_client = boto3.resource("dynamodb")
dynamo_db_table = boto_client.Table("interconnector-data")

def hour_rounder(t):
   # Rounds a datetime to the nearest hour by removing minutes/seconds
   # Used to ensure consistent time boundaries when querying data
   return t.replace(
       second=0, microsecond=0, minute=0, hour=t.hour
   ) + datetime.timedelta(hours=t.minute // 30)

def get_date_keys():
   """Gets timestamps at 15-minute intervals for the past 24 hours"""
   date_keys = []
   # Get current time rounded to hour
   end_range = hour_rounder(datetime.datetime.today())
   # Calculate start time (24 hours ago)
   start_range = end_range - datetime.timedelta(hours=24)
   
   # Generate a timestamp every 15 minutes between start and now
   while start_range <= datetime.datetime.today():
       start_range += datetime.timedelta(minutes=15)
       # Convert datetime to integer format YYYYMMDDHHMMSS
       date_keys.append(int(start_range.strftime("%Y%m%d%H%M%S")))
   return date_keys

def convert_to_epoch(interconnector_data):
   """Converts datetime strings to epoch timestamps for JavaScript frontend"""
   try:
       # Loop through each record in the response
       for i in range(len(interconnector_data["Responses"]["interconnector-data"])):
           # Convert string timestamp to datetime object
           time_stamp = datetime.datetime.strptime(
               str(interconnector_data["Responses"]["interconnector-data"][i]["datetime"]),
               "%Y%m%d%H%M%S",
           )
           # Convert datetime to epoch timestamp
           interconnector_data["Responses"]["interconnector-data"][i][
               "datetime"
           ] = calendar.timegm(time_stamp.timetuple())
       return interconnector_data
   except Exception as e:
       raise Exception(f"Error converting to epoch: {str(e)}")

def get_todays_data_from_db():
   """Fetches and sorts last 24 hours of interconnector data from DynamoDB"""
   try:
       # Batch get items using composite key of datetime and date
       data = boto_client.batch_get_item(
           RequestItems={
               "interconnector-data": {
                   "Keys": [
                       {"datetime": dt, "date": int(str(dt)[:8])} for dt in get_date_keys()
                   ]
               }
           }
       )
       # Ensure we have data and sort it chronologically
       if "Responses" in data and "interconnector-data" in data["Responses"]:
           data["Responses"]["interconnector-data"] = sorted(
               data["Responses"]["interconnector-data"], key=lambda x: x["datetime"]
           )
           # Convert timestamps to epoch format
           data = convert_to_epoch(data)
           return data
       else:
           raise Exception("No data found in DynamoDB response")
   except Exception as e:
       raise Exception(f"Error fetching data from DynamoDB: {str(e)}")

def lambda_handler(event, context):
   """Main Lambda entry point - handles API requests and responses"""
   try:
       # Fetch the formatted interconnector data
       result = get_todays_data_from_db()

       # Construct API response with CORS headers
       response = {
           "statusCode": 200,
           "headers": {
               "Content-Type": "application/json",
               "Access-Control-Allow-Origin": "*"  # Enable CORS for all origins
           },
           # Convert result to JSON, handling Decimal types
           "body": json.dumps(result, cls=DecimalEncoder)
       }

       return response

   except Exception as e:
       # Log error for CloudWatch monitoring
       print(f"Error: {str(e)}")

       # Return error response with 500 status code
       return {
           "statusCode": 500,
           "headers": {
               "Content-Type": "application/json",
               "Access-Control-Allow-Origin": "*"
           },
           "body": json.dumps({
               "message": f"Error processing request: {str(e)}"
           })
       }
