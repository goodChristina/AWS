# Import required libraries
# This updates the Lambda function with the code necessary to interact with the Entsoe 3rd Party API

import pandas as pd
from entsoe import EntsoePandasClient
import datetime
import boto3
import json
import numpy as np
import requests

# Define country codes used in the ENTSO-E API
# UK country code is separate as it's used in all interconnector queries
uk_country_code = "10YGB----------A"
# Dictionary of connected countries and their ENTSO-E codes
country_code_non_uk_dict = {
    "France": "10YFR-RTE------C",
    "Netherlands": "10YNL----------L",
    "Belgium": "10YBE----------2",
    "Norway": "10YNO-0--------C"
}

# Initialize AWS SSM client to retrieve API key from Parameter Store
ssm_client = boto3.client("ssm")
api_key = ssm_client.get_parameter(Name="entsoe-api-token", WithDecryption=False)

# Initialize ENTSO-E API client with retrieved API key
client = EntsoePandasClient(api_key=api_key["Parameter"]["Value"])

# Initialize DynamoDB client and reference to the table storing interconnector data
boto_client = boto3.resource("dynamodb")
dynamo_db_table = boto_client.Table("interconnector-data")

def test_connection():
    """
    Test connectivity to the ENTSO-E API
    Returns: bool indicating if connection was successful
    """
    try:
        response = requests.get('https://web-api.tp.entsoe.eu', timeout=5)
        print(f"Connection test status code: {response.status_code}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Connection test failed: {str(e)}")
        return False

def get_date_range():
    """
    Calculate the time window for data collection
    Returns: dict with start (24 hours ago) and end (2 hours from now) timestamps
    """
    twenty_four_hours = datetime.datetime.today() - datetime.timedelta(days=1)
    tomorrow = datetime.datetime.today() + datetime.timedelta(hours=2)
    return {
        "start": pd.Timestamp(
            twenty_four_hours.strftime("%Y-%m-%d %H:%M:%S"), tz="UTC"
        ),
        "end": pd.Timestamp(tomorrow.strftime("%Y-%m-%d %H:%M:%S"), tz="UTC"),
    }

def get_net_flow(start_end_range, country_code):
    """
    Retrieve and calculate net power flow between UK and specified country
    Args:
        start_end_range: dict containing start and end timestamps
        country_code: ENTSO-E country code for the interconnected country
    Returns: pandas Series of net flow values (positive = export from UK, negative = import to UK)
    """
    try:
        # Log request details for debugging
        print(f"Making request with parameters:")
        print(f"From: {uk_country_code} -> {country_code}")
        print(f"Start: {start_end_range['start']}")
        print(f"End: {start_end_range['end']}")

        # Get flows out of UK to connected country
        flows_outward_from_uk = client.query_crossborder_flows(
            uk_country_code,
            country_code,
            start=start_end_range["start"],
            end=start_end_range["end"],
            timeout=30
        )
        print("Outward flow query successful")

        # Get flows into UK from connected country
        flows_inward_to_uk = client.query_crossborder_flows(
            country_code,
            uk_country_code,
            start=start_end_range["start"],
            end=start_end_range["end"],
            timeout=30
        )
        print("Inward flow query successful")

        # Calculate net flow (outward - inward)
        return flows_outward_from_uk - flows_inward_to_uk
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {str(e)}")
        print(f"Response content: {e.response.content if hasattr(e, 'response') else 'No response content'}")
        raise
    except Exception as e:
        print(f"Error getting flows for {country_code}: {str(e)}")
        raise

def get_all_interconnector_flows(country_code_non_uk_dict):
    """
    Retrieve flow data for all interconnectors
    Args:
        country_code_non_uk_dict: dictionary of country names and their ENTSO-E codes
    Returns: pandas DataFrame containing all interconnector flows
    """
    all_interconnector_dict = {}
    date_range = get_date_range()
    print(f"Querying data from {date_range['start']} to {date_range['end']}")

    # Iterate through each country and get its interconnector data
    for country in list(country_code_non_uk_dict.keys()):
        print(f"Getting data for {country} ({country_code_non_uk_dict[country]})")
        try:
            all_interconnector_dict[country] = get_net_flow(
                date_range, country_code_non_uk_dict[country]
            )
            print(f"Successfully got data for {country}")
        except Exception as e:
            print(f"Failed to get data for {country}: {str(e)}")
            raise

    # Combine all flows into a single DataFrame and forward fill any missing values
    return pd.concat(all_interconnector_dict, axis=1).ffill()

def convert_df_datetime_to_strftime(interconnector_flow_df):
    """
    Convert DataFrame datetime index to string format for database storage
    Args:
        interconnector_flow_df: DataFrame with datetime index
    Returns: DataFrame with formatted datetime strings
    """
    new_df = interconnector_flow_df.reset_index().rename(columns={"index": "datetime"})
    new_df["date"] = new_df["datetime"].apply(lambda x: int(x.strftime("%Y%m%d")))
    new_df["datetime"] = new_df["datetime"].apply(
        lambda x: int(x.strftime("%Y%m%d%H%M%S"))
    )
    return new_df

def convert_float_to_int(interconnector_flow_df):
    """
    Convert float values to integers and handle infinite values
    Args:
        interconnector_flow_df: DataFrame with flow values
    Returns: DataFrame with integer values
    """
    interconnector_flow_df = interconnector_flow_df.replace([np.inf, -np.inf], np.nan)  # replace inf and -inf with NaN
    interconnector_flow_df = interconnector_flow_df.fillna(value=0)  # replace NaN with 0
    return interconnector_flow_df.astype(int)

def convert_df_to_json(interconnector_flow_df):
    """
    Convert DataFrame to JSON format for DynamoDB storage
    Args:
        interconnector_flow_df: DataFrame to convert
    Returns: JSON formatted data
    """
    return json.loads(interconnector_flow_df.reset_index().to_json(orient="records"))

def check_if_exists(input):
    """
    Check if a record already exists in DynamoDB
    Args:
        input: dict containing datetime and date keys
    Returns: bool indicating if record exists
    """
    response = dynamo_db_table.get_item(
        Key={"datetime": input["datetime"], "date": input["date"]}
    )
    if "Item" in response:
        return True
    return False

def put_into_dynamo_db(input):
    """
    Insert a single record into DynamoDB if it doesn't already exist
    Args:
        input: dict containing record to insert
    """
    if check_if_exists(input) == False:
        dynamo_db_table.put_item(Item=input)

def update_dynamo_db(interconnector_dict):
    """
    Update DynamoDB with new interconnector flow records
    Args:
        interconnector_dict: list of dicts containing flow records
    """
    # TODO: Could be optimized using batch operations
    for entry in interconnector_dict:
        put_into_dynamo_db(entry)

def lambda_handler(event, context):
    """
    Main AWS Lambda handler function
    Args:
        event: AWS Lambda event object
        context: AWS Lambda context object
    Returns: dict containing status code and response message
    """
    try:
        # Get data for configured time range
        date_range = get_date_range()
        print(f"Testing API access for date range: {date_range['start']} to {date_range['end']}")

        # Retrieve and process interconnector flow data
        interconnector_df = get_all_interconnector_flows(country_code_non_uk_dict)
        interconnector_df = convert_float_to_int(interconnector_df)
        interconnector_df = convert_df_datetime_to_strftime(interconnector_df)
        interconnector_df.set_index("datetime", inplace=True)
        
        # Convert to JSON and update database
        interconnector_dict = convert_df_to_json(interconnector_df)
        update_dynamo_db(interconnector_dict)
        
        return {"statusCode": 200, "body": json.dumps("Everything works!")}
    except requests.exceptions.HTTPError as e:
        # Handle API authentication errors separately
        if e.response.status_code == 403:
            error_msg = {
                "error": "API Authentication Failed",
                "details": str(e),
                "possible_causes": [
                    "API key not correctly set in SSM",
                    "API key expired or invalid",
                    "IP address blocked",
                    "Request parameters invalid"
                ]
            }
        else:
            error_msg = {"error": str(e)}
        return {
            "statusCode": 500,
            "body": json.dumps(error_msg)
        }

# Script execution entry point for local testing
if __name__ == "__main__":
    try:
        print("Starting test run...")
        print(f"Using API key: {api_key[:8]}...")  # Only show first 8 chars for security
        result = lambda_handler("","")
        print(f"Result: {json.dumps(result, indent=2)}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
