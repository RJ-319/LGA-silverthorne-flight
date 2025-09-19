import os, requests, json
from datetime import date, timedelta

# --- CONFIG ---
BASE_URL = "https://test.api.amadeus.com"
CLIENT_ID = os.environ.get("AMADEUS_CLIENT_ID")
CLIENT_SECRET = os.environ.get("AMADEUS_CLIENT_SECRET")

def get_token():
    r = requests.post(f"{BASE_URL}/v1/security/oauth2/token",
                      data={"grant_type":"client_credentials",
                            "client_id":CLIENT_ID,
                            "client_secret":CLIENT_SECRET})
    r.raise_for_status()
    return r.json()["access_token"]

def search_flights(token, dep_date, ret_date):
    params = {
        "originLocationCode": "LGA",
        "destinationLocationCode": "DEN",
        "departureDate": dep_date,
        "returnDate": ret_date,
        "adults": 2,
        "nonStop": "true",
        "currencyCode": "USD",
        "max": 10
    }
    r = requests.get(f"{BASE_URL}/v2/shopping/flight-offers",
                     headers={"Authorization": f"Bearer {token}"},
                     params=params)
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    token = get_token()
    today = date.today()
    # pick a Thursday two weeks out and Sunday following
    next_thu = today + timedelta((3 - today.weekday()) % 7 + 14)
    next_sun = next_thu + timedelta(days=3)
    offers = search_flights(token, str(next_thu), str(next_sun))
    with open("latest_fares.json", "w") as f:
        json.dump(offers, f, indent=2)
    print(f"Fetched {len(offers.get('data', []))} offers. Saved to latest_fares.json")
