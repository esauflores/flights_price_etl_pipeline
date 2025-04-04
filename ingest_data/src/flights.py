import os
import requests
from typing import Optional
from uuid import uuid4

from src.auth import get_auth_token_amadeus
from src.bucket import save_json_to_gcs

def get_flights_amadeus(
    origin: str,
    destination: str,
    departure_date: str,
    currency_code: str = "USD",
    save_to_gcs: bool = False,
):
    url = os.getenv("AMADEUS_FLIGHTS_OFFERS_URL")
    headers = {
        "Authorization": f"Bearer {get_auth_token_amadeus()}"
    }
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": destination,
        "departureDate": departure_date,
        "adults": 1,
        "currencyCode": currency_code,
        "nonStop": 'true'
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        raise Exception(f"Failed to get flights data {response.json()}")
    
    if save_to_gcs:
        id = f"{origin}_{destination}_{departure_date}_{currency_code}"
        filename = f"raw_data/flights_{id}.json"
        save_json_to_gcs(response.json(), filename)

    return response.json().get("data")