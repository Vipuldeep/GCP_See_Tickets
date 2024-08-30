# Cloud Funtion Name see_tickets_to_bigquery

import functions_framework
import requests
import json
from google.cloud import bigquery
from google.cloud import secretmanager
from datetime import datetime

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function to fetch data from See Tickets API and insert it into BigQuery.
    Args:
        request (flask.Request): The request object.
    Returns:
        The response text indicating success or failure.
    """
    
    # Declared project, dataset, and table names
    project_id = "otis-media"  
    dataset_id = "seetickets_data"  
    table_id = f"{project_id}.{dataset_id}.sales"

    # Initialize BigQuery client with explicit project ID
    client = bigquery.Client(project=project_id)

    # Fetch unique identifiers from the table (e.g., uniqueCode)
    fetch_existing_ids_query = f"SELECT uniqueCode FROM `{table_id}`"
    
    try:
        query_job = client.query(fetch_existing_ids_query)  # Make an API request.
        results = query_job.result()  # Wait for the job to complete.

        # Store existing unique codes in a set for fast lookup
        existing_unique_codes = {row.uniqueCode for row in results}
        
        # Log the number of existing records for debugging
        print(f"Number of existing records in {table_id}: {len(existing_unique_codes)}")
    
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
    url = "https://clients-api.seetickets.com/v1/reports/sales"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Payload for the API request
    payload = {
        "offset": 0,
        "search": {
            "forSearchItemType": "EVENT",
            "searchItemCodes": ['DF-2974218', 'DF-2974224', 'DF-2974228', 'DF-2974229', 'DF-2974230', 'DF-2974231', 'DF-2974360', 'DF-2974361', 'DF-2974362', 'DF-2974363']
        }
    }
    
    # Make the API request
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    
    # Handle the response
    if response.status_code == 200:
        try:
            response_json = response.json()  # Attempt to parse the JSON response
            
            # Logging the response for debugging
            print(f"API Response: {json.dumps(response_json, indent=4)}")
            
            # Prepare rows for BigQuery, only add new records
            rows_to_insert = []
            for record in response_json.get('data', []):
                unique_code = record.get("uniqueCode")
                
                # Check if the record already exists
                if unique_code not in existing_unique_codes:
                    row = {
                        "affiliate": record.get("affiliate"),
                        "currency": record.get("currency"),
                        "date": record.get("date"),
                        "deliveryMethod": record.get("deliveryMethod"),
                        "deviceModel": record.get("deviceModel"),
                        "deviceType": record.get("deviceType"),
                        "eventDate": record.get("eventDate"),
                        "eventId": record.get("eventId"),
                        "eventName": record.get("eventName"),
                        "faceValue": record.get("faceValue"),
                        "grossCost": record.get("grossCost"),
                        "grossFace": record.get("grossFace"),
                        "grossSales": record.get("grossSales"),
                        "isInternetOrder": record.get("isInternetOrder"),
                        "isPending": record.get("isPending"),
                        "country": record.get("location", {}).get("country"),
                        "countryIso": record.get("location", {}).get("countryIso"),
                        "postCode": record.get("location", {}).get("postCode"),
                        "region1": record.get("location", {}).get("region1"),
                        "region2": record.get("location", {}).get("region2"),
                        "region3": record.get("location", {}).get("region3"),
                        "region4": record.get("location", {}).get("region4"),
                        "offerCode": record.get("offerCode"),
                        "partnerSite": record.get("partnerSite"),
                        "paymentMethod": record.get("paymentMethod"),
                        "priceId": record.get("priceId"),
                        "priceName": record.get("priceName"),
                        "salesChannel": record.get("salesChannel"),
                        "sold": record.get("sold"),
                        "source": record.get("source"),
                        "taxTotal": record.get("taxTotal"),
                        "ticketPrice": record.get("ticketPrice"),
                        "uniqueCode": unique_code
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
