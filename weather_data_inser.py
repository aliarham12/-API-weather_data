from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List
from pymongo import MongoClient
from datetime import datetime, timedelta
import requests

app = FastAPI()

# Function to fetch weather data from the API
def fetch_weather_data(api_key, location, date):
    url = f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={date}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        weather_data = response.json()
        return weather_data
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data for {location} on {date}: {e}")

# Function to insert data into MongoDB
def insert_into_mongodb(data, db_name, collection_name):
    try:
        client = MongoClient('localhost', 27017)
        db = client[db_name]
        collection = db[collection_name]
        # Insert only if the data for this date doesn't exist
        date = data['forecast']['forecastday'][0]['date']
        existing_entry = collection.find_one({"forecast.forecastday.date": date})
        if not existing_entry:
            collection.insert_one(data)
            return f"Data for {date} inserted successfully into {collection_name}!"
        else:
            return f"Data for {date} already exists in {collection_name}, skipping insertion."
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting data into MongoDB: {e}")

# Function to fetch and store data in MongoDB
def fetch_and_store_data(api_key, location, start_date, end_date):
    client = MongoClient('localhost', 27017)
    db = client["weather_forecast_db"]
    collection = db[f"{location.lower()}_forecast"]
    
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    results = []

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")

        # Check if the data for this date already exists in MongoDB
        existing_entry = collection.find_one({"forecast.forecastday.date": date_str})
        
        if not existing_entry:
            weather_data = fetch_weather_data(api_key, location, date_str)
            result = insert_into_mongodb(weather_data, db_name="weather_forecast_db", collection_name=f"{location.lower()}_forecast")
            results.append(result)
        else:
            results.append(f"Data for {date_str} already exists in {location.lower()}_forecast, skipping API call.")
        
        current_date += timedelta(days=1)

    return results

# Define the FastAPI endpoint to trigger the data fetch and store process
@app.post("/fetch_weather_data/")
def fetch_weather(
    api_key: str,
    locations: Optional[List[str]] = Query(None),
    end_date: Optional[str] = None,
    start_date: Optional[str] = None
):
    if locations is None:
        locations = ['Karachi', 'Islamabad', 'Lahore']

    # Use current date as end_date if not provided
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Use one year before the end date as start_date if not provided
    if start_date is None:
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")

    all_results = {}

    for location in locations:
        results = fetch_and_store_data(api_key, location, start_date, end_date)
        all_results[location] = results

    return {"message": "Data fetching and storing process completed!", "results": all_results}
