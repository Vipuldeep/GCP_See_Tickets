# Cloud Funtion Name events

import functions_framework
import requests
import json
from google.cloud import bigquery
from google.cloud import secretmanager
from datetime import datetime

@functions_framework.http
def events(request):
    """HTTP Cloud Function to fetch event data from See Tickets API and insert it into BigQuery.
    Args:
        request (flask.Request): The request object.
    Returns:
        The response text indicating success or failure.
    """

    # Declared project, dataset, and table names
    project_id = "otis-media"
    dataset_id = "seetickets_data"
    table_id = f"{project_id}.{dataset_id}.events"

    # Initialize BigQuery client with explicit project ID
    client = bigquery.Client(project=project_id)

    # Fetch unique identifiers from the table (e.g., id)
    fetch_existing_ids_query = f"SELECT id FROM `{table_id}`"
    
    try:
        query_job = client.query(fetch_existing_ids_query)  # Make an API request.
        results = query_job.result()  # Wait for the job to complete.

        # Store existing IDs in a set for fast lookup
        existing_ids = {row.id for row in results}
        
        # Log the number of existing records for debugging
        print(f"Number of existing records in {table_id}: {len(existing_ids)}")
    
    except Exception as e:
        print(f"Error fetching existing records: {e}")
        return f"Error fetching existing records: {e}", 500

    # Initialize Secret Manager client
    secret_client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret
    secret_name = f"projects/{project_id}/secrets/SEE_TICKETS_API_KEY/versions/latest"
    
    try:
        # Access the secret version
        response = secret_client.access_secret_version(name=secret_name)
        api_key = response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error accessing secret: {e}")
        return f"Error accessing secret: {e}", 500

    # URL and headers for the API request
    url = "https://clients-api.seetickets.com/v1/events/search"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Payload for the API request
    payload = {
        "offset": 0,
        # "limit": 100 
    }
    
    # Make the API request with GET method
    response = requests.get(url, headers=headers, params=payload)
    
    # Handle the response
    if response.status_code == 200:
        try:
            response_json = response.json()  # Attempt to parse the JSON response
            
            # Logging the response for debugging
            print(f"API Response: {json.dumps(response_json, indent=4)}")
            
            # Prepare rows for BigQuery, only add new records
            rows_to_insert = []
            for record in response_json.get('data', []):
                event_id = record.get("id")
                
                # Check if the record already exists
                if event_id not in existing_ids:
                    row = {
                        "doorsOpen": record.get("doorsOpen"),
                        "id": event_id,
                        "name": record.get("name"),
                        "promoterName": record.get("promoterName"),
                        "starts": record.get("starts"),
                        "timeslotEnabled": record.get("timeslotEnabled"),
                        "tourId": record.get("tourId"),
                        "tourName": record.get("tourName"),
                        "venueName": record.get("venueName")
                    }
                    rows_to_insert.append(row)
            
            # Insert new data into BigQuery
            if rows_to_insert:
                errors = client.insert_rows_json(table_id, rows_to_insert)
                
                if errors:
                    print(f"BigQuery Insertion Errors: {errors}")
                    return f"Encountered errors while inserting rows: {errors}", 500
                
                print(f"Successfully inserted {len(rows_to_insert)} new rows.")
                return f"Successfully inserted {len(rows_to_insert)} new rows.", 200
            else:
                print("No new data to insert.")
                return "No new data to insert.", 200
        
        except ValueError as e:
            # Handle JSON parsing error
            print(f"Error parsing JSON response: {e}")
            return f"Error parsing JSON response: {e}", 500
        except Exception as e:
            # Catch-all for any other exceptions
            print(f"An unexpected error occurred: {e}")
            return f"An unexpected error occurred: {e}", 500
    else:
        print(f"Request failed: {response.status_code} {response.text}")
        return f"Request failed: {response.status_code} {response.text}", response.status_code
