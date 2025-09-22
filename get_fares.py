import os, json, time
from datetime import date, timedelta, datetime, time as dtime
from statistics import median

import pytz
import requests
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ----------------- CONFIG -----------------
BASE_URL = "https://test.api.amadeus.com"
CLIENT_ID = os.environ["AMADEUS_CLIENT_ID"]
CLIENT_SECRET = os.environ["AMADEUS_CLIENT_SECRET"]

NYC_OUTBOUND_AIRPORTS = ["LGA", "JFK"]   # try either for Thu departure
NYC_RETURN_AIRPORTS   = ["LGA", "JFK"]   # allow return to either on Sun
DEN = "DEN"

ADULTS = 2
CURRENCY = "USD"
NONSTOP_ONLY = True

# Airline preferences / filters
PREFERRED_AIRLINES = ["DL"]            # try first
SECONDARY_AIRLINES = ["AA", "UA", "B6"]
BLOCKED_AIRLINES = []                  # e.g., ["F9","NK"]

# Time windows (local airport times)
NY_TZ = pytz.timezone("America/New_York")
DEN_TZ = pytz.timezone("America/Denver")
OUTBOUND_EARLIEST_ET = dtime(18, 0)         # Thu after 6:00 PM ET
RETURN_WINDOW_START_MT = dtime(12, 0)       # Sun ≥12:00 PM MT
RETURN_WINDOW_END_MT   = dtime(15, 30)      # Sun ≤3:30 PM MT

# Near-term scan (~2 months)
NEAR_TERM_WEEKS = 9

# Rare-deal scan (~6 months; bump to 12 if desired)
RARE_LOOKAHEAD_MONTHS = 6
RARE_MIN_DROP = 0.35           # include if >=35% below baseline
RARE_MAX_RESULTS = 3

# HTTP timeouts & retries
POST_TIMEOUT = 20
GET_TIMEOUT  = 20
RETRY_TOTAL = 3
RETRY_BACKOFF = 1.0
RETRY_STATUSES = (429, 500, 502, 503, 504)
# ------------------------------------------

# --------- robust requests session ---------
def build_session():
    retry = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_TOTAL,
        read=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=RETRY_STATUSES,
        allowed_methods={"GET", "POST"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

S = build_session()

# -------- token cache / refresh ------------
_TOKEN = None
_TOKEN_EXP = 0  # epoch seconds

def _fetch_token():
    r = S.post(
        f"{BASE_URL}/v1/security/oauth2/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=POST_TIMEOUT,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Amadeus token error {r.status_code}: {r.text}")
    data = r.json()
    tok = data["access_token"]
    ttl = int(data.get("expires_in", 1800)) - 120  # refresh a bit early
    exp = int(time.time()) + max(300, ttl)
    return tok, exp

def get_token():
    global _TOKEN, _TOKEN_EXP
    _TOKEN, _TOKEN_EXP = _fetch_token()
    return _TOKEN

def ensure_token():
    now = int(time.time())
    if not _TOKEN or now >= _TOKEN_EXP:
        return get_token()
    return _TOKEN

# --------------- helpers -------------------
def parse_iso_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def is_nonstop_offer(offer):
    # one-way itinerary must have exactly 1 segment
    for itin in offer["itineraries"]:
        if len(itin["segments"]) != 1:
            return False
    return True

def times_ok_oneway(offer, is_outbound):
    seg = offer["itineraries"][0]["segments"][0]
    dep = parse_iso_dt(seg["departure"]["at"])
    if is_outbound:
        dep_local = NY_TZ.normalize(dep.astimezone(NY_TZ)) if dep.tzinfo else NY_TZ.localize(dep)
        return dep_local.time() >= OUTBOUND_EARLIEST_ET
    else:
        dep_local = DEN_TZ.normalize(dep.astimezone(DEN_TZ)) if dep.tzinfo else DEN_TZ.localize(dep)
        return RETURN_WINDOW_START_MT <= dep_local.time() <= RETURN_WINDOW_END_MT

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

def amadeus_search_oneway(dep_date, origin, dest, include_codes=None, exclude_codes=None):
    """GET /v2/shopping/flight-offers with oneWay=true"""
    tok = ensure_token()
    params = {
        "originLocationCode": origin,
        "destinationLocationCode": dest,
        "departureDate": dep_date,
        "adults": ADULTS,
        "currencyCode": CURRENCY,
        "max": 50,
        "oneWay": "true",
    }
    if NONSTOP_ONLY:
        params["nonStop"] = "true"
    if include_codes:
        params["includedAirlineCodes"] = ",".join(include_codes)

    def call(tk):
        return S.get(
            f"{BASE_URL}/v2/shopping/flight-offers",
            headers={"Authorization": f"Bearer {tk}"},
            params=params,
            timeout=GET_TIMEOUT,
        )

    try:
        r = call(tok)
        if r.status_code == 401:
            tok = get_token()
            r = call(tok)
        r.raise_for_status()
        data = r.json().get("data", [])
    except requests.RequestException as e:
        print(f"[warn] oneway request failed {origin}->{dest} {dep_date}: {e}")
        return []

    if exclude_codes:
        excl = set(exclude_codes)
        def first_carrier(off):
            return off["itineraries"][0]["segments"][0]["carrierCode"]
        data = [o for o in data if first_carrier(o) not in excl]

    return data

def best_oneway_by_cabin(dep_date, origin, dest, is_outbound):
    """
    Return cheapest one-way per cabin (ECONOMY, PREMIUM_ECONOMY, FIRST)
    that passes nonstop + time windows, preferring PREFERRED/SECONDARY airlines.
    """
    winners = {}

    def consider(off):
        if NONSTOP_ONLY and not is_nonstop_offer(off):
            return
        if not times_ok_oneway(off, is_outbound=is_outbound):
            return
        cab = cabin_from_offer(off)
        price = total_price_for_two(off)
        if cab not in winners or price < winners[cab]["price_total_2"]:
            seg = off["itineraries"][0]["segments"][0]
            winners[cab] = {
                "price_total_2": price,
                "airline": airline_code(off),
                "segment": {
                    "dep": seg["departure"]["at"],
                    "arr": seg["arrival"]["at"],
                    "from": seg["departure"]["iataCode"],
                    "to": seg["arrival"]["iataCode"],
                },
            }

    # First pass: preferred + secondary
    first_pass = list(dict.fromkeys(PREFERRED_AIRLINES + SECONDARY_AIRLINES))
    for off in amadeus_search_oneway(dep_date, origin, dest, include_codes=first_pass, exclude_codes=BLOCKED_AIRLINES):
        consider(off)

    # Fallback: any except blocked
    if not winners:
        for off in amadeus_search_oneway(dep_date, origin, dest, include_codes=None, exclude_codes=BLOCKED_AIRLINES):
            consider(off)

    return winners  # may be empty

def generate_thu_sun_pairs(n_weeks, start_from=None):
    today = start_from or date.today()
    days_to_thu = (3 - today.weekday()) % 7  # Mon=0
    first_thu = today + timedelta(days=days_to_thu)
    for w in range(n_weeks):
        thu = first_thu + timedelta(weeks=w)
        sun = thu + timedelta(days=3)
        yield thu, sun

def generate_thu_sun_pairs_months(months):
    today = date.today()
    end_date = today + relativedelta(months=+months)
    days_to_thu = (3 - today.weekday()) % 7
    cur_thu = today + timedelta(days=days_to_thu)
    while cur_thu < end_date:
        yield cur_thu, cur_thu + timedelta(days=3)
        cur_thu += timedelta(weeks=1)

# ------------ combine OW legs into RT totals ------------
CABINS = ["ECONOMY", "PREMIUM_ECONOMY", "FIRST"]

def combine_roundtrip_for_weekend(thu, sun):
    dep_date = str(thu)
    ret_date = str(sun)

    # For each cabin, find cheapest outbound (NYC->DEN) & return (DEN->NYC) across LGA/JFK
    winners_rt = {c: None for c in CABINS}

    # Precompute best outbound per cabin per origin
    best_out_per_origin = {}
    for o in NYC_OUTBOUND_AIRPORTS:
        best_out_per_origin[o] = best_oneway_by_cabin(dep_date, o, DEN, is_outbound=True)

    # Precompute best return per cabin per return-airport
    best_ret_per_dest = {}
    for d in NYC_RETURN_AIRPORTS:
        best_ret_per_dest[d] = best_oneway_by_cabin(ret_date, DEN, d, is_outbound=False)

    for cabin in CABINS:
        best_total = None
        best_combo = None

        for o in NYC_OUTBOUND_AIRPORTS:
            out_cand = best_out_per_origin[o].get(cabin)
            if not out_cand:
                continue
            for d in NYC_RETURN_AIRPORTS:
                ret_cand = best_ret_per_dest[d].get(cabin)
                if not ret_cand:
                    continue
                total = out_cand["price_total_2"] + ret_cand["price_total_2"]
                if best_total is None or total < best_total:
                    best_total = total
                    best_combo = (o, d, out_cand, ret_cand)

        if best_combo:
            o, d, out_cand, ret_cand = best_combo
            winners_rt[cabin] = {
                "price_total_2": round(best_total, 2),
                "airline_out": out_cand["airline"],
                "airline_ret": ret_cand["airline"],
                "outbound": {
                    "dep": out_cand["segment"]["dep"],
                    "arr": out_cand["segment"]["arr"],
                    "from": out_cand["segment"]["from"],
                    "to":   out_cand["segment"]["to"],
                },
                "return": {
                    "dep": ret_cand["segment"]["dep"],
                    "arr": ret_cand["segment"]["arr"],
                    "from": ret_cand["segment"]["from"],
                    "to":   ret_cand["segment"]["to"],
                },
            }

    def p(c): return winners_rt.get(c, {}).get("price_total_2")
    min_price = min([x for x in [p("ECONOMY"), p("PREMIUM_ECONOMY"), p("FIRST")] if x is not None], default=None)

    return {
        "ECONOMY": winners_rt["ECONOMY"],
        "PREMIUM_ECONOMY": winners_rt["PREMIUM_ECONOMY"],
        "FIRST": winners_rt["FIRST"],
        "min_price": min_price,
    }

# ------------------- main scans -------------------
def daily_scan():
    weekends = list(generate_thu_sun_pairs(NEAR_TERM_WEEKS))
    near_results = []
    for thu, sun in weekends:
        try:
            winners = combine_roundtrip_for_weekend(thu, sun)
        except Exception as e:
            print(f"[warn] near-term weekend {thu}->{sun} failed: {e}")
            winners = {"ECONOMY": None, "PREMIUM_ECONOMY": None, "FIRST": None, "min_price": None}
        near_results.append({
            "weekend": {"thu": str(thu), "sun": str(sun)},
            "winners": winners,
        })
    candidates = [r for r in near_results if r["winners"]["min_price"] is not None]
    best = min(candidates, key=lambda r: r["winners"]["min_price"]) if candidates else None
    return near_results, best

def build_rare_deals():
    """Median baseline of ECONOMY OW+OW totals; flag big relative drops."""
    records = []  # {"weekend":(thu,sun), "econ_total":float, "winner":dict}
    for thu, sun in generate_thu_sun_pairs_months(RARE_LOOKAHEAD_MONTHS):
        try:
            winners = combine_roundtrip_for_weekend(thu, sun)
        except Exception as e:
            print(f"[warn] rare weekend {thu}->{sun} failed: {e}")
            continue
        econ = winners.get("ECONOMY")
        if econ and winners["min_price"] is not None:
            records.append({
                "weekend": (thu, sun),
                "econ_total": float(econ["price_total_2"]),
                "winner": econ
            })

    if len(records) < 6:
        return []

    base = median(r["econ_total"] for r in records)
    if base <= 0:
        return []

    outliers = []
    for r in records:
        drop = (base - r["econ_total"]) / base
        if drop >= RARE_MIN_DROP:
            thu, sun = r["weekend"]
            w = r["winner"]
            outliers.append({
                "weekend": {"thu": str(thu), "sun": str(sun)},
                "cabin": "ECONOMY",
                "price_total_2": round(r["econ_total"], 2),
                "pct_below_baseline": round(drop, 4),
                # include per-leg airlines and airports (may differ)
                "airline_out": w.get("airline_out"),
                "airline_ret": w.get("airline_ret"),
                "outbound": w["outbound"],
                "return":  w["return"],
            })

    outliers.sort(key=lambda x: (-x["pct_below_baseline"], x["price_total_2"]))
    return outliers[:RARE_MAX_RESULTS]

def main():
    # validate/prime token
    get_token()

    # Near-term daily pick
    near_results, best = daily_scan()

    # Year-ahead rare deals
    rare = build_rare_deals()

    output = {
        "route": "NYC(LGA/JFK)-DEN mixable",
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
