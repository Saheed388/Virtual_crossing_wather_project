import requests
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Define your Azure Storage account details


# Create a connection to the Azure Storage account

def append_to_azure_append_blob(data, blob_client):
    try:
        blob_client.create_append_blob()
        blob_client.append_block(data)
        blob_client.flush_append_blob()
        print(f"Data appended to Azure Append Blob: {blob_client.blob_name}")
    except Exception as e:
        print(f"Failed to append data to Azure Append Blob: {str(e)}")

def get_weather_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch weather data. Status Code: {response.status_code}")
        return None

base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
api_key = "************************88"  # Replace with your API key

zip_codes = [
     "01105", "02110", "02114", "02129", "06355", "06854", "07305", "10001", "10017", "10024",
      "10036", "10128", "11225", "17325", "19103", "19104", "19106", "20001", "20004", "20006",
        "20565", "21202", "22060", "22172", "29401", "30313", "30315", "32803", "32837", "32940",
        "33405", "33602", "33767", "34112", "34236", "37402", "39501", "43204", "44106", "44113",
        "44114", "46222", "48124", "60022", "60605", "60614", "60637", "63017", "63110", "64108",
         "74037", "75202", "75207", "78205", "78402", "80205", "80904", "84020", "85008", "85054",
        "85256", "85716", "89101", "89109", "90007", "90027", "90036", "90731", "92037", "92101",
         "92123", "92260", "92802", "94118", "96707", "96743", "96753", "96761", "96815", "97214",
        "97221", "97365", "98101", "98103", "98328", "98407", "99507", "99664"
    ]

# Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)
start_date = yesterday.strftime('%Y-%m-%d')
end_date = yesterday.strftime('%Y-%m-%d')

all_weather_data = []

for zip_code in zip_codes:
    url = f"{base_url}/{zip_code}/{start_date}/{end_date}?unitGroup=metric&include=hours&key={api_key}&contentType=json"
    weather_data = get_weather_data(url)

    if weather_data:
        # Extract overall weather data
        overall_data = {
            "zip_code": zip_code,
            "query_cost": weather_data.get('queryCost'),
            "resolved_address": weather_data.get('resolvedAddress'),
            "timezone": weather_data.get('timezone'),
            "latitude": weather_data.get('latitude'),
            "longitude": weather_data.get('longitude'),
            "tzoffset": weather_data.get('tzoffset')
        }
        all_weather_data.append(overall_data)

        # Extract hourly weather data
        days_list = weather_data.get("days", [])  # Navigate to the correct structure

        if days_list:
            # Extract hourly weather data
            hours_list = days_list[0].get("hours", [])

            # Assuming data for a single day
            first_day = days_list[0]

            for hour in hours_list:
                row = {
                    "zip_code": zip_code,
                    "datetime": f"{first_day.get('datetime')} {hour.get('datetime')}",
                    "temp": hour.get("temp"),
                    "feelslike": hour.get("feelslike"),
                    "humidity": hour.get("humidity"),
                    "dew": hour.get("dew"),
                    "precip": hour.get("precip"),
                    "precipprob": hour.get("precipprob"),
                    "snow": hour.get("snow"),
                    "snowdepth": hour.get("snowdepth"),
                    "preciptype": hour.get("preciptype"),
                    "windgust": hour.get("windgust"),
                    "windspeed": hour.get("windspeed"),
                    "winddir": hour.get("winddir"),
                    "pressure": hour.get("pressure"),
                    "visibility": hour.get("visibility"),
                    "cloudcover": hour.get("cloudcover"),
                    "solarradiation": hour.get("solarradiation"),
                    "solarenergy": hour.get("solarenergy"),
                    "uvindex": hour.get("uvindex"),
                    "severerisk": hour.get("severerisk"),
                    "conditions": hour.get("conditions"),
                    "icon": hour.get("icon"),
                    "stations": hour.get("stations", []),
                    "source": hour.get("source")
                }
                all_weather_data.append(row)

# Create a DataFrame for the new data
new_data = pd.DataFrame(all_weather_data)

# Convert DataFrame to CSV in memory (without saving to disk)
csv_data = new_data.to_csv(index=False).encode()

# Append the CSV data to Azure Append Blob
