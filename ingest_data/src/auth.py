import requests
import os
import time

auth_token = None
token_expiry_time = None

def get_auth_token_amadeus():
    global auth_token, token_expiry_time

    if (
        auth_token is not None
        and token_expiry_time is not None
        and time.time() < token_expiry_time
    ):
        return auth_token

    url = os.getenv("AMADEUS_AUTH_URL")
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("AMADEUS_CLIENT_ID"),
        "client_secret": os.getenv("AMADEUS_CLIENT_SECRET"),
    }
    response = requests.post(url, headers=headers, data=data)

    if response.status_code != 200:
        raise Exception("Failed to get auth token")

    if not response.json().get("access_token"):
        raise Exception("Failed to get auth token")
    

    auth_token = response.json().get("access_token")
    token_expiry_time = time.time() + response.json().get("expires_in") - 60
    return auth_token
