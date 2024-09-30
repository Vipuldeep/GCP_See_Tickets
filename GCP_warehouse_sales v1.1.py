import functions_framework
import requests
import json
from google.cloud import bigquery
from google.cloud import secretmanager
from datetime import datetime

def create_table_if_not_exists(client, project_id, dataset_id, table_id):
    """Creates a new BigQuery table with a specified schema if it does not exist."""
    schema = [
        bigquery.SchemaField("affiliate", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("deliveryMethod", "STRING"),
        bigquery.SchemaField("deviceModel", "STRING"),
        bigquery.SchemaField("deviceType", "STRING"),
        bigquery.SchemaField("eventDate", "STRING"),
        bigquery.SchemaField("eventId", "STRING"),
        bigquery.SchemaField("eventName", "STRING"),
        bigquery.SchemaField("faceValue", "FLOAT"),
        bigquery.SchemaField("grossCost", "FLOAT"),
        bigquery.SchemaField("grossFace", "FLOAT"),
        bigquery.SchemaField("grossSales", "FLOAT"),
        bigquery.SchemaField("isInternetOrder", "BOOL"),
        bigquery.SchemaField("isPending", "BOOL"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("countryIso", "STRING"),
        bigquery.SchemaField("postCode", "STRING"),
        bigquery.SchemaField("region1", "STRING"),
        bigquery.SchemaField("region2", "STRING"),
        bigquery.SchemaField("region3", "STRING"),
        bigquery.SchemaField("region4", "STRING"),
        bigquery.SchemaField("offerCode", "STRING"),
        bigquery.SchemaField("partnerSite", "STRING"),
        bigquery.SchemaField("paymentMethod", "STRING"),
        bigquery.SchemaField("priceId", "STRING"),
        bigquery.SchemaField("priceName", "STRING"),
        bigquery.SchemaField("salesChannel", "STRING"),
        bigquery.SchemaField("sold", "INTEGER"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("taxTotal", "FLOAT"),
        bigquery.SchemaField("ticketPrice", "FLOAT"),
        bigquery.SchemaField("uniqueCode", "STRING")
    ]

    # Set table reference with the full project, dataset, and table name
    table_ref = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}", schema=schema)
    try:
        client.create_table(table_ref)
        print(f"Created table {table_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"Table {table_id} already exists.")
        else:
            raise e

@functions_framework.http
def warehousesales(request):
    """HTTP Cloud Function to fetch data from See Tickets API and insert it into BigQuery.
    Args:
        request (flask.Request): The request object.
    Returns:
        The response text indicating success or failure.
    """

    # Ensure the function handles GET requests correctly
    if request.method == 'GET':
        return "This function only accepts POST requests.", 405  # Method Not Allowed

    # Declared project, dataset, and table names
    project_id = "see-tickets-433213"
    dataset_id = "seetickets_data"
    table_name = "warehouse_sales"  # Fixed table name

    # Initialize BigQuery client with explicit project ID
    client = bigquery.Client(project=project_id)

    # Create the table if it does not exist
    try:
        create_table_if_not_exists(client, project_id, dataset_id, table_name)
    except Exception as e:
        print(f"Error creating table: {e}")
        return f"Error creating table: {e}", 500

    # Delete existing data from the table
    try:
        query = f"TRUNCATE TABLE `{project_id}.{dataset_id}.{table_name}`"
        query_job = client.query(query)
        query_job.result()  # Waits for job to complete
        print(f"Cleared data from table {table_name}")
    except Exception as e:
        print(f"Error clearing data from table: {e}")
        return f"Error clearing data from table: {e}", 500

    # Initialize Secret Manager client
    secret_client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret
    secret_name = f"projects/{project_id}/secrets/SEE_TICKETS_API_KEY/versions/latest"

    try:
        # Access the secret version
        response = secret_client.access_secret_version(name=secret_name)
        api_key = response.payload.data.decode("UTF-8")
        print("Successfully accessed the API key from Secret Manager.")
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
    try:
        print("Sending request to See Tickets API...")
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        print(f"Received response with status code {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return f"Error making API request: {e}", 500

    # Handle the response
    if response.status_code == 200:
        try:
            response_json = response.json()  # Attempt to parse the JSON response

            # Logging the response for debugging
            print(f"API Response: {json.dumps(response_json, indent=4)}")

            # Prepare rows for BigQuery
            rows_to_insert = []
            for record in response_json.get('data', []):
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
                    "uniqueCode": record.get("uniqueCode")
                }
                rows_to_insert.append(row)

            # Insert new data into BigQuery
            if rows_to_insert:
                print(f"Inserting {len(rows_to_insert)} rows into BigQuery table {table_name}...")
                # Use the fully qualified table ID, including project, dataset, and table name
                errors = client.insert_rows_json(f"{project_id}.{dataset_id}.{table_name}", rows_to_insert)

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
