import os, json, requests
from datetime import date, timedelta, datetime, time
from dateutil.relativedelta import relativedelta
import pytz

# ----------------- CONFIG -----------------
BASE_URL = "https://test.api.amadeus.com"
CLIENT_ID = os.environ["AMADEUS_CLIENT_ID"]
CLIENT_SECRET = os.environ["AMADEUS_CLIENT_SECRET"]

ORIGIN = "LGA"
DEST = "DEN"
ADULTS = 2
CURRENCY = "USD"
NONSTOP_ONLY = True
DELTA_FIRST = True  # try DL first, then fallback to any airline

# time windows (local airport times)
NY_TZ = pytz.timezone("America/New_York")
DEN_TZ = pytz.timezone("America/Denver")
OUTBOUND_EARLIEST_ET = time(18, 0)            # Thu after 6:00 PM ET
RETURN_WINDOW_START_MT = time(12, 0)          # Sun 12:00 PM MT
RETURN_WINDOW_END_MT   = time(15, 30)         # Sun 3:30 PM MT

# how many Thu→Sun weekends to scan
NEAR_TERM_WEEKS = 9  # ~2 months
# ------------------------------------------


def get_token():
    r = requests.post(
        f"{BASE_URL}/v1/security/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def amadeus_search(token, dep_date, ret_date, included_airline_codes=None):
    """Return raw offers for our pair of dates."""
    params = {
        "originLocationCode": ORIGIN,
        "destinationLocationCode": DEST,
        "departureDate": dep_date,
        "returnDate": ret_date,
        "adults": ADULTS,
        "currencyCode": CURRENCY,
        "max": 50,
    }
    if NONSTOP_ONLY:
        params["nonStop"] = "true"
    if included_airline_codes:
        params["includedAirlineCodes"] = included_airline_codes

    r = requests.get(
        f"{BASE_URL}/v2/shopping/flight-offers",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=60,
    )
    # If no data, Amadeus still returns 200 with empty 'data'
    r.raise_for_status()
    return r.json().get("data", [])


def parse_amadeus_dt(s):
    # "2025-11-06T18:35:00" or with offset; treat as naive local per segment
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def is_nonstop_offer(offer):
    # Each direction should have exactly 1 segment
    for itin in offer["itineraries"]:
        if len(itin["segments"]) != 1:
            return False
    return True


def times_ok(offer):
    # outbound is itinerary[0], return is itinerary[1]
    out_seg = offer["itineraries"][0]["segments"][0]
    ret_seg = offer["itineraries"][1]["segments"][0]

    out_dep = parse_amadeus_dt(out_seg["departure"]["at"])
    ret_dep = parse_amadeus_dt(ret_seg["departure"]["at"])

    # Convert to local airport tzs
    out_dep_local = NY_TZ.normalize(out_dep.astimezone(NY_TZ)) if out_dep.tzinfo else NY_TZ.localize(out_dep)
    ret_dep_local = DEN_TZ.normalize(ret_dep.astimezone(DEN_TZ)) if ret_dep.tzinfo else DEN_TZ.localize(ret_dep)

    if out_dep_local.time() < OUTBOUND_EARLIEST_ET:
        return False
    if not (RETURN_WINDOW_START_MT <= ret_dep_local.time() <= RETURN_WINDOW_END_MT):
        return False
    return True


def total_price_for_two(offer):
    # grandTotal is the total for all passengers; verify ADULTS=2 and use grandTotal
    return float(offer["price"]["grandTotal"])


def airline_name_from_offer(offer):
    # pull carrier code of the first segment
    code = offer["itineraries"][0]["segments"][0]["carrierCode"]
    return code  # keep IATA code (DL/UA/etc.). Mapping to full name is optional.


def cabin_from_offer(offer):
    # Offers can include mixed cabins; use fareDetailsBySegment of first segment outbound
    try:
        fd = offer["travelerPricings"][0]["fareDetailsBySegment"][0]
        return fd.get("cabin", "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def generate_thu_sun_pairs(n_weeks):
    # Start from upcoming Thursday (including this week if still upcoming)
    today = date.today()
    days_to_thu = (3 - today.weekday()) % 7  # Monday=0,... Thursday=3
    first_thu = today + timedelta(days=days_to_thu)
    for w in range(n_weeks):
        thu = first_thu + timedelta(weeks=w)
        sun = thu + timedelta(days=3)
        yield thu, sun


def cheapest_by_cabin(token, thu, sun):
    # Try Delta first (if configured), then fallback to any airline
    iso_dep, iso_ret = str(thu), str(sun)
    offers = []

    if DELTA_FIRST:
        offers = amadeus_search(token, iso_dep, iso_ret, included_airline_codes="DL")
    if not offers:
        offers = amadeus_search(token, iso_dep, iso_ret, included_airline_codes=None)

    # Filter: nonstop + time windows
    filtered = []
    for off in offers:
        if NONSTOP_ONLY and not is_nonstop_offer(off):
            continue
        if not times_ok(off):
            continue
        filtered.append(off)

    # Pick cheapest per cabin among filtered
    winners = {}  # {"ECONOMY": {...}, "PREMIUM_ECONOMY": {...}, "FIRST": {...}}
    for off in filtered:
        cabin = cabin_from_offer(off)
        price = total_price_for_two(off)
        if cabin not in winners or price < winners[cabin]["price_total_2"]:
            out_seg = off["itineraries"][0]["segments"][0]
            ret_seg = off["itineraries"][1]["segments"][0]
            winners[cabin] = {
                "price_total_2": price,
                "airline": airline_name_from_offer(off),
                "outbound": {
                    "dep": out_seg["departure"]["at"],
                    "arr": out_seg["arrival"]["at"],
                    "from": out_seg["departure"]["iataCode"],
                    "to": out_seg["arrival"]["iataCode"],
                },
                "return": {
                    "dep": ret_seg["departure"]["at"],
                    "arr": ret_seg["arrival"]["at"],
                    "from": ret_seg["departure"]["iataCode"],
                    "to": ret_seg["arrival"]["iataCode"],
                },
            }

    # Normalize keys we care about
    def get_price(cabin):
        return winners.get(cabin, {}).get("price_total_2")

    return {
        "ECONOMY": winners.get("ECONOMY"),
        "PREMIUM_ECONOMY": winners.get("PREMIUM_ECONOMY"),
        "FIRST": winners.get("FIRST"),
        "min_price": min([p for p in [get_price("ECONOMY"), get_price("PREMIUM_ECONOMY"), get_price("FIRST")] if p is not None], default=None),
    }


def main():
    token = get_token()

    weekends = list(generate_thu_sun_pairs(NEAR_TERM_WEEKS))
    results = []
    for thu, sun in weekends:
        winners = cheapest_by_cabin(token, thu, sun)
        results.append({
            "weekend": {"thu": str(thu), "sun": str(sun)},
            "winners": winners,
        })

    # choose the overall-cheapest weekend by its min_price
    candidates = [r for r in results if r["winners"]["min_price"] is not None]
    best = min(candidates, key=lambda r: r["winners"]["min_price"]) if candidates else None

    output = {
        "route": f"{ORIGIN}-{DEST}",
        "pax": ADULTS,
        "currency": CURRENCY,
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "near_term_scan": results,       # all weekends scanned (compact)
        "daily_pick": best               # the cheapest qualifying weekend across cabins
    }

    # Write to /docs for GitHub Pages
    os.makedirs("docs", exist_ok=True)
    with open("docs/latest_fares.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved docs/latest_fares.json ({'found' if best else 'no best match'})")


if __name__ == "__main__":
    main()
