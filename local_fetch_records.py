import requests
import json

def fetch_all_records():
    url = "https://clients-api.seetickets.com/v1/events/search"
    headers = {
        "Authorization": "Bearer {{API_KEY}}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to fetch records. Status code: {response.status_code}")
        return None

def beautify_json(data):
    return json.dumps(data, indent=4, sort_keys=True)

if __name__ == "__main__":
    records = fetch_all_records()
    if records:
        pretty_records = beautify_json(records)
        print(pretty_records)
