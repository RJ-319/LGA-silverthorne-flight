import os, json, requests
from datetime import date, timedelta, datetime, time
from dateutil.relativedelta import relativedelta
import pytz
from statistics import median

# ----------------- CONFIG -----------------
BASE_URL = "https://test.api.amadeus.com"
CLIENT_ID = os.environ["AMADEUS_CLIENT_ID"]
CLIENT_SECRET = os.environ["AMADEUS_CLIENT_SECRET"]

ORIGIN = "LGA"
DEST = "DEN"
ADULTS = 2
CURRENCY = "USD"
NONSTOP_ONLY = True

# Airline preferences / filters
PREFERRED_AIRLINES = ["DL"]               # try these first
SECONDARY_AIRLINES = ["AA", "UA", "B6"]   # then try these
BLOCKED_AIRLINES = []                     # e.g., ["F9","NK"] to exclude ULCCs

# Time windows (local airport times)
NY_TZ = pytz.timezone("America/New_York")
DEN_TZ = pytz.timezone("America/Denver")
OUTBOUND_EARLIEST_ET = time(18, 0)            # Thu after 6:00 PM ET
RETURN_WINDOW_START_MT = time(12, 0)          # Sun 12:00 PM MT
RETURN_WINDOW_END_MT   = time(15, 30)         # Sun 3:30 PM MT

# Near-term scan (~2 months)
NEAR_TERM_WEEKS = 9

# Rare-deal scan (~12 months) and trigger
RARE_LOOKAHEAD_MONTHS = 12
RARE_MIN_DROP = 0.35          # include if >=35% below baseline (relative, not absolute)
RARE_MAX_RESULTS = 3
# ------------------------------------------


def get_token():
    r = requests.post(
        f"{BASE_URL}/v1/security/oauth2/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials",
              "client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Amadeus token error {r.status_code}: {r.text}")
    return r.json()["access_token"]


def amadeus_search(token, dep_date, ret_date, include_codes=None, exclude_codes=None):
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
    if include_codes:
        params["includedAirlineCodes"] = ",".join(include_codes)

    r = requests.get(
        f"{BASE_URL}/v2/shopping/flight-offers",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=60,
    )
    r.raise_for_status()
    data = r.json().get("data", [])

    if exclude_codes:
        def first_carrier(off):
            return off["itineraries"][0]["segments"][0]["carrierCode"]
        data = [o for o in data if first_carrier(o) not in set(exclude_codes)]

    return data


def parse_amadeus_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def is_nonstop_offer(offer):
    for itin in offer["itineraries"]:
        if len(itin["segments"]) != 1:
            return False
    return True


def times_ok(offer):
    out_seg = offer["itineraries"][0]["segments"][0]
    ret_seg = offer["itineraries"][1]["segments"][0]
    out_dep = parse_amadeus_dt(out_seg["departure"]["at"])
    ret_dep = parse_amadeus_dt(ret_seg["departure"]["at"])
    out_dep_local = NY_TZ.normalize(out_dep.astimezone(NY_TZ)) if out_dep.tzinfo else NY_TZ.localize(out_dep)
    ret_dep_local = DEN_TZ.normalize(ret_dep.astimezone(DEN_TZ)) if ret_dep.tzinfo else DEN_TZ.localize(ret_dep)
    if out_dep_local.time() < OUTBOUND_EARLIEST_ET:
        return False
    if not (RETURN_WINDOW_START_MT <= ret_dep_local.time() <= RETURN_WINDOW_END_MT):
        return False
    return True


def total_price_for_two(offer):
    return float(offer["price"]["grandTotal"])


def airline_code(offer):
    return offer["itineraries"][0]["segments"][0]["carrierCode"]


def cabin_from_offer(offer):
    try:
        fd = offer["travelerPricings"][0]["fareDetailsBySegment"][0]
        return fd.get("cabin", "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def generate_thu_sun_pairs(n_weeks, start_from=None):
    today = start_from or date.today()
    days_to_thu = (3 - today.weekday()) % 7  # Mon=0
    first_thu = today + timedelta(days=days_to_thu)
    for w in range(n_weeks):
        thu = first_thu + timedelta(weeks=w)
        sun = thu + timedelta(days=3)
        yield thu, sun


def generate_thu_sun_pairs_months(months):
    # From next Thu until months ahead
    today = date.today()
    end_date = today + relativedelta(months=+months)
    days_to_thu = (3 - today.weekday()) % 7
    cur_thu = today + timedelta(days=days_to_thu)
    while cur_thu < end_date:
        yield cur_thu, cur_thu + timedelta(days=3)
        cur_thu += timedelta(weeks=1)


def find_offers(token, thu, sun):
    dep, ret = str(thu), str(sun)

    # 1) Preferred
    offers = amadeus_search(token, dep, ret, include_codes=PREFERRED_AIRLINES, exclude_codes=BLOCKED_AIRLINES)
    # 2) Secondary
    if not offers and SECONDARY_AIRLINES:
        offers = amadeus_search(token, dep, ret, include_codes=SECONDARY_AIRLINES, exclude_codes=BLOCKED_AIRLINES)
    # 3) Any
    if not offers:
        offers = amadeus_search(token, dep, ret, include_codes=None, exclude_codes=BLOCKED_AIRLINES)

    # Filter
    filt = []
    for off in offers:
        if NONSTOP_ONLY and not is_nonstop_offer(off):
            continue
        if not times_ok(off):
            continue
        filt.append(off)
    return filt


def extract_winners_by_cabin(filtered_offers):
    winners = {}
    for off in filtered_offers:
        cab = cabin_from_offer(off)
        price = total_price_for_two(off)
        if cab not in winners or price < winners[cab]["price_total_2"]:
            out_seg = off["itineraries"][0]["segments"][0]
            ret_seg = off["itineraries"][1]["segments"][0]
            winners[cab] = {
                "price_total_2": price,
                "airline": airline_code(off),
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
    def get_price(c):
        return winners.get(c, {}).get("price_total_2")
    return {
        "ECONOMY": winners.get("ECONOMY"),
        "PREMIUM_ECONOMY": winners.get("PREMIUM_ECONOMY"),
        "FIRST": winners.get("FIRST"),
        "min_price": min([p for p in [get_price("ECONOMY"), get_price("PREMIUM_ECONOMY"), get_price("FIRST")] if p is not None], default=None),
    }


def cheapest_by_cabin(token, thu, sun):
    filtered = find_offers(token, thu, sun)
    return extract_winners_by_cabin(filtered)


def build_rare_deals(token):
    """Scan ~12 months; build baseline (median of all economy prices),
       and emit entries ≥ RARE_MIN_DROP below baseline."""
    records = []  # list of {"weekend":(thu,sun), "econ_price":float, "winner":dict}
    for thu, sun in generate_thu_sun_pairs_months(RARE_LOOKAHEAD_MONTHS):
        winners = cheapest_by_cabin(token, thu, sun)
        econ = winners.get("ECONOMY")
        if econ and winners["min_price"] is not None:
            records.append({
                "weekend": (thu, sun),
                "econ_price": float(econ["price_total_2"]),
                "winner": econ
            })

    if len(records) < 6:
        return []  # not enough data to form a baseline

    baseline = median(r["econ_price"] for r in records)
    if baseline <= 0:
        return []

    outliers = []
    for r in records:
        drop = (baseline - r["econ_price"]) / baseline  # relative
        if drop >= RARE_MIN_DROP:
            thu, sun = r["weekend"]
            w = r["winner"]
            outliers.append({
                "weekend": {"thu": str(thu), "sun": str(sun)},
                "cabin": "ECONOMY",
                "price_total_2": round(r["econ_price"], 2),
                "pct_below_baseline": round(drop, 4),
                "airline": w.get("airline"),
                "outbound": w["outbound"],
                "return": w["return"],
            })

    # Sort: biggest drop first, then cheapest absolute price
    outliers.sort(key=lambda x: (-x["pct_below_baseline"], x["price_total_2"]))
    return outliers[:RARE_MAX_RESULTS]


def main():
    token = get_token()

    # --- Near-term scan for daily_pick ---
    weekends = list(generate_thu_sun_pairs(NEAR_TERM_WEEKS))
    near_results = []
    for thu, sun in weekends:
        winners = cheapest_by_cabin(token, thu, sun)
        near_results.append({
            "weekend": {"thu": str(thu), "sun": str(sun)},
            "winners": winners,
        })
    candidates = [r for r in near_results if r["winners"]["min_price"] is not None]
    best = min(candidates, key=lambda r: r["winners"]["min_price"]) if candidates else None

    # --- Year-ahead outliers ---
    rare = build_rare_deals(token)

    output = {
        "route": f"{ORIGIN}-{DEST}",
        "pax": ADULTS,
        "currency": CURRENCY,
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "near_term_scan": near_results,
        "daily_pick": best,
        "rare_deals": rare
    }

    os.makedirs("docs", exist_ok=True)
    with open("docs/latest_fares.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"Saved docs/latest_fares.json | daily_pick: {'yes' if best else 'no'} | rare_deals: {len(rare)}")


if __name__ == "__main__":
    main()
