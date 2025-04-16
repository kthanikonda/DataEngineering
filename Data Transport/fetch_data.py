import urllib.request
import json

vehicle_ids = [3212, 2904] 

base_url = "https://busdata.cs.pdx.edu/api/getBreadCrumbs"

vehicle_data = []

for vid in vehicle_ids:
    url = f"{base_url}?vehicle_id={vid}"
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            vehicle_data.extend(data)  
        print(f"Data for Vehicle ID {vid} fetched successfully.")
    except Exception as e:
        print(f"Failed to fetch data for Vehicle ID {vid}: {e}")

with open("bcsample.json", 'w') as outfile:
    json.dump(vehicle_data, outfile, indent=4)

print("Sample data saved in 'bcsample.json'.")
