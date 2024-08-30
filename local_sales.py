import requests
import json

# Define the URL and headers
url = "https://clients-api.seetickets.com/v1/reports/sales"
headers = {
    "Authorization": "Bearer {{API_KEY}}",
    "Content-Type": "application/json"
}

# Define the payload
payload = {
    # "filteredBy": {
    #     "salesStartDate": "2024-01-01T00:00:00Z",
    #     "salesEndDate": "2024-12-31T23:59:59Z"
    # },
    # "limit": 100,
    "offset": 0,
    "search": {
        "forSearchItemType": "EVENT",
        "searchItemCodes": ['DF-2974218', 'DF-2974224', 'DF-2974228', 'DF-2974229', 'DF-2974230', 'DF-2974231', 'DF-2974360', 'DF-2974361', 'DF-2974362', 'DF-2974363']
    }
}

# Convert the payload to a JSON string with indentation for beautification
payload_json = json.dumps(payload, indent=4)

# Make the POST request
response = requests.post(url, headers=headers, data=payload_json)

# Check if the response is in JSON format and pretty print it
if response.headers.get('Content-Type') == 'application/json':
    response_json = response.json()
    pretty_response = json.dumps(response_json, indent=4)
    print("Status Code:", response.status_code)
    print("Response Body:", pretty_response)
else:
    print("Status Code:", response.status_code)
    print("Response Body:", response.text)
