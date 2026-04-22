#!/usr/bin/env python3
"""
Washington County Incident Ledger - Data Fetcher

Downloads the weekly RMS incident CSVs from Washington County MN Sheriff's
open data portal, caches them locally, combines into one dataset, and writes
a data.js file that the dashboard reads.

Usage:
    python3 fetch_data.py

First run downloads the full historical archive (roughly 180+ files, a few
minutes). Subsequent runs only pull new weekly files that aren't in cache.

No external dependencies. Pure Python 3 stdlib.
"""

import csv
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

RMS_INDEX = "https://web1.co.washington.mn.us/MediaReports/RMS/"
HERE = Path(__file__).resolve().parent
CACHE_DIR = HERE / "cache"
DATA_JS = HERE / "data.js"

# Categorization. Substring match, lowercased. ORDER MATTERS - more specific first.
CATEGORY_PATTERNS = [
    # Specific crime categories first
    ("DUI",            ["dui", "dwi", "impaired driver"]),
    ("Vehicle Crime",  ["motor vehicle theft", "auto theft", "mvt",
                        "theft from motor", "theft from vehicle", "theft from auto",
                        "vehicle break", "vehicle tampering", "tampering"]),
    ("Burglary",       ["burglary"]),
    ("Robbery",        ["robbery"]),
    ("Homicide",       ["homicide", "murder"]),
    ("Kidnapping",     ["kidnapping", "abduction"]),
    ("Suicide / Suicidal", ["suicide", "suicidal", "person in crisis",
                            "emotionally disturb", "mental health", "mental crisis"]),
    ("CPS Referral",   ["cps referral", "child protective"]),
    ("MAARC",          ["maarc", "mn adult abuse", "minnesota adult abuse"]),
    ("Domestic Violence",["domestic violence", "domestic assault", "domestic abuse"]),
    ("Arson",          ["arson"]),
    ("Order Violation",["ofp violation", "hro violation", "danco", "no contact order",
                        "court order violation", "protection order", "judges order",
                        "conditional release violation"]),
    ("Death",          ["death investigation", "death", "deceased person", "drowning"]),
    ("Fraud",          ["fraud", "identity", "forgery", "check forgery",
                        "scam", "embezzle", "counterfeit", "internet crime"]),
    ("Drugs",          ["drug", "narcotic", "controlled substance", "marijuana", "paraphernalia"]),
    ("Weapons",        ["weapon", "firearm", "shots fired", "shots heard", "shooting"]),
    ("Threat",         ["threat", "terroristic"]),
    ("Harassment",     ["harassment", "harassing", "stalking"]),
    ("Trespass",       ["trespass", "unwanted person", "unwanted vehicle"]),
    ("Sex Offense",    ["indecent exposure", "peeping", "lewd"]),
    ("Theft",          ["theft", "larceny", "shoplifting", "stolen", "gas drive off",
                        "drive off"]),
    ("Assault",        ["assault", "fight", "stabbing"]),
    ("Crash",          ["crash", "mva", "pi accident", "pd accident",
                        "property damage accident", "hit and run", "hit & run",
                        "vehicle off road", "off road", "accident"]),
    ("Vandalism",      ["vandalism", "damage to property", "property damage",
                        "criminal damage", "graffiti"]),
    # Dispatch / service event types
    ("Traffic",        ["traffic stop", "traffic offense", "traffic complaint",
                        "traffic hazard", "traffic control", "driving complaint",
                        "road hazard", "boat stop", "stat radar", "stationary radar",
                        "school crossing", "disabled vehicle", "stop arm", "atv", "ohv",
                        "watercraft", "pursuit", "speed", "reckless",
                        "expired registration", "expired reg", "snowmobile",
                        "failure to drive", "use of comm dev", "watercraft violation",
                        "w/w expired", "v/w ", "w/w ", "cite -", "cite-",
                        "no proof of ins", "dar"]),
    ("Alarm",          ["alarm"]),
    ("Animal",         ["animal", "dog at large", "dog bite", "deer", "deceased animal",
                        "loose dog", "dangerous dog", "found dog", "lost dog",
                        "potentially dangerous"]),
    ("Medical",        ["medical", "ems assist", "overdose", "ambulance",
                        "emergency exam", "72 hr hold", "72-hour", "transport hold",
                        "fall", "sick person", "difficulty breathing"]),
    ("Welfare Check",  ["welfare check", "welfare / check", "well-being", "wellbeing"]),
    ("Suspicious",     ["suspicious"]),
    ("Civil",          ["civil", "neighbor dispute", "child custody", "paper service",
                        "court order"]),
    ("Lockout",        ["lockout", "lock-out", "vehicle unlock", "auto unlock", "unlock"]),
    ("Property",       ["found property", "lost property", "recovered property"]),
    ("Noise",          ["noise"]),
    ("Parking",        ["parking", "abandoned vehicle"]),
    ("911",            ["911 hang", "911 open", "911 abandoned", "open line"]),
    ("Missing Person", ["missing person", "runaway", "missing juvenile", "found person",
                        "lost person"]),
    ("Disturbance",    ["disturb", "disorderly", "loud party", "fireworks",
                        "drunk", "intoxicated", "underage drinking", "nuisance",
                        "solicitor complaint"]),
    ("Fire",           ["fire", "smoke", "burning complaint", "burn complaint",
                        "gas leak", "electrical hazard"]),
    ("Warrant",        ["warrant", "wanted person", "kops", "atl",
                        "fugitive", "probation violation", "por violation"]),
    ("Juvenile",       ["juvenile", "school/student", "school student", "curfew violation"]),
    ("Ordinance",      ["ordinance violation", "dumping complaint", "dumping",
                        "littering", "train complaint", "hunting complaint",
                        "city ord"]),
    ("Follow Up",      ["follow up", "follow-up", "followup"]),
    ("Service Request",["public works", "park close", "park check", "park incident",
                        "park complaint", "community contact", "ordinance",
                        "fingerprint", "water incident", "weather incident",
                        "water complaint", "gun permit", "license",
                        "aircraft/drone", "drone complaint", "aircraft complaint",
                        "ufo", "honor box", "data request", "solicitor permit",
                        "construction check"]),
    ("Officer Init.",  ["officer initiated", "self initiated", "directed patrol",
                        "premise check", "business check", "foot patrol",
                        "open door", "open window", "officer information",
                        "assigned & cleared", "assigned and cleared",
                        "compliance check", "ride along", "officer complaint",
                        "misc info", "area check", "snowbird warning",
                        "vacation check", "school check", "k9 demo",
                        "srt", "cnt call out", "swat"]),
    ("Assist",         ["assist", "motorist", "public service"]),
]

# Visual tier grouping for the dashboard. Order = display order.
# "Off The Ledger" is the headline tier: categories the county docs
# explicitly say are excluded from the public feed but appear anyway.
CATEGORY_TIERS = [
    ("Off The Ledger", ["Suicide / Suicidal", "CPS Referral",
                        "MAARC", "Domestic Violence"]),
    ("Index Crimes",   ["Homicide", "Kidnapping", "Robbery", "Assault",
                        "Burglary", "Theft", "Vehicle Crime", "Arson", "Sex Offense"]),
    ("Public Order",   ["DUI", "Drugs", "Weapons", "Threat", "Harassment",
                        "Trespass", "Fraud", "Vandalism", "Order Violation",
                        "Death", "Disturbance", "Missing Person", "Noise"]),
    ("Service Calls",  ["Medical", "Traffic", "Crash", "Alarm", "911",
                        "Welfare Check", "Animal", "Suspicious", "Civil",
                        "Lockout", "Property", "Parking", "Fire",
                        "Juvenile", "Ordinance", "Service Request"]),
    ("Procedural",     ["Follow Up", "Officer Init.", "Assist"]),
    ("Unclassified",   ["Other", "Unknown"]),
]

# City name normalization. Keys are uppercase, stripped.
CITY_ALIASES = {
    "OAK PARK HTS":          "Oak Park Heights",
    "OAK PARK HEIGHTS":      "Oak Park Heights",
    "ST PAUL PARK":          "Saint Paul Park",
    "ST. PAUL PARK":         "Saint Paul Park",
    "SAINT PAUL PARK":       "Saint Paul Park",
    "ST CROIX BEACH":        "Lake St. Croix Beach",
    "LAKE ST CROIX BCH":     "Lake St. Croix Beach",
    "LAKE ST. CROIX BEACH":  "Lake St. Croix Beach",
    "MARINE":                "Marine on St. Croix",
    "MARINE ON ST CRX":      "Marine on St. Croix",
    "MAY TWP":               "May Township",
    "BAYTOWN TWP":           "Baytown Township",
    "DENMARK TWP":           "Denmark Township",
    "STILLWATER TWP":        "Stillwater Township",
    "WEST LAKELAND":         "West Lakeland",
    "WEST LAKELAND TWP":     "West Lakeland",
    "GREY CLOUD":            "Grey Cloud Island",
    "GREY CLOUD ISLAND TWP": "Grey Cloud Island",
}


def normalize_city(raw):
    if not raw:
        return "Unknown"
    upper = raw.strip().upper()
    if upper in CITY_ALIASES:
        return CITY_ALIASES[upper]
    return raw.strip().title()


def log(msg):
    print(f"[ledger] {msg}", file=sys.stderr)


def list_remote_files():
    """Scrape the open directory listing to get all CSV filenames."""
    log("fetching directory listing...")
    req = urllib.request.Request(RMS_INDEX, headers={"User-Agent": "wc-ledger/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    # The listing is plain text inside a <pre>; filenames look like IncidentSummary_YYYYMMDD.csv
    names = sorted(set(re.findall(r"IncidentSummary_\d{8}\.csv", html)))
    log(f"found {len(names)} weekly files on server")
    return names


def download(filename):
    """Download one CSV to cache/ unless it's already there."""
    dest = CACHE_DIR / filename
    if dest.exists() and dest.stat().st_size > 0:
        return dest, False  # already cached
    url = RMS_INDEX + filename
    req = urllib.request.Request(url, headers={"User-Agent": "wc-ledger/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
        dest.write_bytes(data)
        return dest, True
    except urllib.error.URLError as e:
        log(f"  failed {filename}: {e}")
        return None, False


def categorize(incident_type):
    if not incident_type or incident_type.strip().lower() in ("unknown", ""):
        return "Unknown"
    low = incident_type.strip().lower()
    for bucket, patterns in CATEGORY_PATTERNS:
        if any(p in low for p in patterns):
            return bucket
    return "Other"


def parse_date(value):
    """Try a few common formats. Returns ISO date (YYYY-MM-DD) or None."""
    dt = parse_datetime(value)
    return dt[0] if dt else None


def parse_datetime(value):
    """Returns (iso_date, hour) tuple or None."""
    if not value:
        return None
    value = value.strip()
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p",
                "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M",
                "%Y-%m-%d %H:%M:%S", "%m/%d/%y %H:%M"):
        try:
            d = datetime.strptime(value, fmt)
            return (d.date().isoformat(), d.hour)
        except ValueError:
            continue
    # Date-only fallbacks (no hour info)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            d = datetime.strptime(value, fmt)
            return (d.date().isoformat(), None)
        except ValueError:
            continue
    # Last resort: yank the first date-like substring
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", value)
    if m:
        mo, da, yr = m.groups()
        if len(yr) == 2:
            yr = "20" + yr
        try:
            return (datetime(int(yr), int(mo), int(da)).date().isoformat(), None)
        except ValueError:
            return None
    return None


def find_col(headers, candidates, exclude=None):
    """Find the first header that matches any candidate (case-insensitive).
    Tries exact match first, then substring match. Optionally excludes columns
    already claimed by another field."""
    exclude = exclude or set()
    low_headers = [h.lower().strip() for h in headers]
    # First pass: exact equality
    for cand in candidates:
        for i, h in enumerate(low_headers):
            if i in exclude:
                continue
            if h == cand:
                return i
    # Second pass: substring match
    for cand in candidates:
        for i, h in enumerate(low_headers):
            if i in exclude:
                continue
            if cand in h:
                return i
    return None


def parse_csv(path):
    """Parse one weekly CSV. Returns list of dicts with normalized fields."""
    rows = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            try:
                headers = next(reader)
            except StopIteration:
                return rows
            col_date = find_col(headers, ["createdon1", "createdon", "reported", "occurred",
                                          "incident date", "date"])
            col_city = find_col(headers, ["address_city_description1", "address_city_description",
                                          "city_description", "address_city", "city",
                                          "municipality", "jurisdiction"])
            col_type = find_col(headers, ["eventdescription", "event description", "incident type",
                                          "offense", "call type", "description", "type"],
                                exclude={col_city} if col_city is not None else None)
            claimed = {x for x in (col_date, col_type, col_city) if x is not None}
            col_loc  = find_col(headers, ["textbox28", "address_streetaddress", "address",
                                          "location", "street", "block"],
                                exclude=claimed)
            col_case = find_col(headers, ["casenum", "case_number", "casenumber",
                                          "case num", "case", "incident number",
                                          "report number", "id"])

            for r in reader:
                if not r or all(not c for c in r):
                    continue
                def g(i):
                    return r[i].strip() if i is not None and i < len(r) else ""
                dt = parse_datetime(g(col_date))
                if not dt:
                    continue
                date_str, hour = dt
                itype = g(col_type) or "Unknown"
                rows.append({
                    "date":     date_str,
                    "hour":     hour,
                    "type":     itype,
                    "city":     normalize_city(g(col_city)),
                    "location": g(col_loc),
                    "case":     g(col_case),
                    "category": categorize(itype),
                })
    except Exception as e:
        log(f"  parse error {path.name}: {e}")
    return rows


def iso_week(date_str):
    """Return Monday-of-week ISO date for a YYYY-MM-DD string."""
    d = datetime.fromisoformat(date_str).date()
    monday = d.fromordinal(d.toordinal() - d.weekday())
    return monday.isoformat()


def ym(date_str):
    return date_str[:7]  # YYYY-MM


# Primary pattern - matches variants of speed citations with XX/YY format
# that include the SPEED or SPEEDING keyword.
# The [-\u2013\u2014?] char class handles hyphen, en-dash, em-dash, and the
# literal "?" that some encoders produce for mangled dashes.
SPEED_CITE_RX = re.compile(
    r"(?:CITE[DX]?\s*[-\u2013\u2014?]?\s*)?SPEED(?:ING)?\s*[-\u2013\u2014?]?\s*(\d{2,3})\s*/\s*(\d{2,3})(.*)",
    re.IGNORECASE
)

# Secondary pattern - CITE/CITED directly followed by XX/YY with no SPEED
# keyword. In the context of a traffic-category dispatch entry that starts
# with CITE, "45/30" is almost certainly speed/limit.
CITE_NUMBERS_RX = re.compile(
    r"^\s*CITE[DX]?\s*[-\u2013\u2014?]?\s*(\d{2,3})\s*/\s*(\d{2,3})(.*)",
    re.IGNORECASE
)

# Fallback natural-language pattern: "45 IN A 30", "45 IN 30"
SPEED_IN_RX = re.compile(r"\b(\d{2,3})\s+IN\s+(?:A\s+)?(\d{2,3})\b", re.IGNORECASE)

def parse_speed_cite(event_type):
    if not event_type:
        return None
    s = event_type.strip().upper()
    # Primary: explicit SPEED/SPEEDING keyword
    m = SPEED_CITE_RX.search(s)
    if m:
        try:
            speed = int(m.group(1))
            limit = int(m.group(2))
            if 10 <= limit <= 80 and 10 <= speed <= 140 and speed >= limit:
                tail = m.group(3).strip().lstrip(",").strip() or ""
                return speed, limit, tail
        except ValueError:
            pass
    # Secondary: CITE + XX/YY with no SPEED keyword
    m = CITE_NUMBERS_RX.match(s)
    if m:
        try:
            speed = int(m.group(1))
            limit = int(m.group(2))
            if 10 <= limit <= 80 and 10 <= speed <= 140 and speed > limit:
                tail = m.group(3).strip().lstrip(",").strip() or ""
                return speed, limit, tail
        except ValueError:
            pass
    # Fallback - only if the surrounding text references speed/speeding
    if "SPEED" in s:
        m = SPEED_IN_RX.search(s)
        if m:
            try:
                speed = int(m.group(1))
                limit = int(m.group(2))
                if 10 <= limit <= 80 and 10 <= speed <= 140 and speed > limit:
                    return speed, limit, ""
            except ValueError:
                pass
    return None


# Classifier patterns for speed-related STOPS that don't have numbers.
# These still represent real traffic stops - warnings, generic citations,
# enforcement details. Volume is significant (~500+ entries).
SPEED_STOP_PATTERNS = [
    ("Written Warning",      re.compile(r"\b(?:W\s*/\s*W|W\.W|WW)\s*[-\u2013\u2014?/\.]?\s*SPEED", re.IGNORECASE)),
    ("Verbal Warning",       re.compile(r"\b(?:V\s*/\s*W|VW)\s*[-\u2013\u2014?/\.\s]?\s*SPEED", re.IGNORECASE)),
    ("Traffic Stop (speed)", re.compile(r"\bTS\s*/\s*SPEED", re.IGNORECASE)),
    ("Cite, no speed logged",re.compile(r"\bCIT(?:E|ED|)?\s*[-\u2013\u2014?/\.]?\s*SPEED\b(?!\s*[-\u2013\u2014?]?\s*\d)", re.IGNORECASE)),
    ("Enforcement detail",   re.compile(r"\b(?:SPEED\s+(?:ENFORCEMENT|DETAIL|TRAILER|SIGN)|DIRECTED\s+PATROL\s*[-\u2013\u2014?]?\s*SPEED)", re.IGNORECASE)),
]

def classify_speed_stop(event_type):
    """Return a label for speed-related non-citation stops, or None."""
    if not event_type:
        return None
    s = event_type.strip().upper()
    if "SPEED" not in s:
        return None
    # If we already parse it as a numbered citation, not a warning
    if parse_speed_cite(s):
        return None
    for label, rx in SPEED_STOP_PATTERNS:
        if rx.search(s):
            return label
    return None


def aggregate_rare_codes(rows, threshold=10):
    """Find every event-type string that appears <= threshold times total.
    For each rare code, collect all its incidents. This surfaces the codes
    that are effectively invisible: rare enough to never be news, yet real
    events recorded in the data."""
    counts = defaultdict(int)
    by_type = defaultdict(list)
    for r in rows:
        t = (r.get("type") or "").strip().upper()
        if not t or t == "UNKNOWN":
            continue
        counts[t] += 1
        by_type[t].append(r)

    rare_list = [(t, n) for t, n in counts.items() if n <= threshold]
    # Sort by count ascending (rarest first), then alpha for stable ordering
    rare_list.sort(key=lambda x: (x[1], x[0]))

    result = []
    for code, count in rare_list:
        instances = sorted(by_type[code], key=lambda r: r["date"], reverse=True)
        # Determine the dominant category for this code
        cat_counts = defaultdict(int)
        for r in instances:
            cat_counts[r.get("category") or "Unknown"] += 1
        dominant_cat = max(cat_counts.items(), key=lambda x: x[1])[0]

        result.append({
            "code": code,
            "count": count,
            "category": dominant_cat,
            "instances": [
                {
                    "date":     r["date"],
                    "hour":     r.get("hour"),
                    "city":     r["city"],
                    "location": r.get("location", ""),
                    "case":     r.get("case", ""),
                    "category": r.get("category", ""),
                    "type":     r["type"],
                }
                for r in instances
            ],
        })
    return result


def aggregate_traffic(rows):
    """Parse CITE - SPEED strings, return enforcement summary or None."""
    # Overall enforcement volume: every row in the Traffic category
    traffic_rows = [r for r in rows if r.get("category") == "Traffic"]
    total_traffic = len(traffic_rows)

    # Generic "TRAFFIC STOP" - officer-initiated stop with no outcome recorded.
    # This is the biggest bucket in Traffic and the honesty gap: we know a stop
    # happened but not whether it resulted in a warning, citation, or nothing.
    generic_stops = sum(
        1 for r in traffic_rows
        if (r.get("type") or "").strip().upper() == "TRAFFIC STOP"
    )

    # Break down all citation-prefixed codes (CITE - XXXXX) so we know what
    # other enforcement activity exists alongside speed citations.
    cite_type_counts = defaultdict(int)
    cite_rx = re.compile(r"^\s*CITE\s*[-\u2013]?\s*", re.IGNORECASE)
    for r in traffic_rows:
        t = (r.get("type") or "").strip().upper()
        if cite_rx.match(t):
            # Strip the "CITE - " prefix, keep the rest as-is
            rest = cite_rx.sub("", t).strip()
            # Group "SPEED" variants together
            if rest.startswith("SPEED") or rest.startswith("SPEEDING"):
                key = "SPEED"
            else:
                # Group by first token to reduce cardinality
                # ("STOP SIGN" and "STOP SIGN VIOLATION" both -> "STOP SIGN")
                parts = rest.split(",")[0].strip()
                key = parts[:60]  # cap to reasonable width
            cite_type_counts[key] += 1

    cite_breakdown = sorted(
        [{"type": k, "count": v} for k, v in cite_type_counts.items()],
        key=lambda x: -x["count"]
    )[:30]

    # Speed-related stops WITHOUT extractable numbers (warnings, generic cites)
    # These are real stops that happened - we just can't compute deltas for them.
    speed_stops_no_num = []
    stop_label_counts = defaultdict(int)
    for row in traffic_rows:
        label = classify_speed_stop(row.get("type") or "")
        if label:
            stop_label_counts[label] += 1
            speed_stops_no_num.append({
                "date":     row["date"],
                "city":     row["city"],
                "type":     row["type"],
                "label":    label,
                "location": row.get("location", ""),
                "case":     row.get("case", ""),
            })

    warnings_summary = {
        "total": len(speed_stops_no_num),
        "byLabel": sorted(
            [{"label": k, "count": v} for k, v in stop_label_counts.items()],
            key=lambda x: -x["count"]
        ),
        "byCity": None,  # computed below
        "stops": speed_stops_no_num,
    }
    # By city for warnings
    wc = defaultdict(int)
    for s in speed_stops_no_num:
        if s["city"] and s["city"].lower() not in ("", "unknown"):
            wc[s["city"]] += 1
    warnings_summary["byCity"] = sorted(
        [{"city": k, "count": v} for k, v in wc.items()],
        key=lambda x: -x["count"]
    )[:30]

    # Speed citations
    cites = []
    unmatched_types = defaultdict(int)
    for row in traffic_rows:
        parsed = parse_speed_cite(row.get("type", ""))
        if not parsed:
            # Track unmatched codes for diagnostic
            t = (row.get("type") or "").strip().upper()
            if t and "SPEED" in t:
                unmatched_types[t] += 1
            continue
        speed, limit, tail = parsed
        cites.append({
            "date":     row["date"],
            "hour":     row.get("hour"),
            "city":     row["city"],
            "speed":    speed,
            "limit":    limit,
            "delta":    speed - limit,
            "tail":     tail,
            "type":     row["type"],
            "location": row.get("location", ""),
            "case":     row.get("case", ""),
            "category": row.get("category", ""),
        })

    # Diagnostic: print unmatched speed-related codes
    if unmatched_types:
        log(f"unmatched codes containing 'SPEED' (top 20):")
        for t, n in sorted(unmatched_types.items(), key=lambda x: -x[1])[:20]:
            log(f"   {n:>5}  {t}")

    # Filter the 6-over anomaly (CITE - SPEED 26/20 appears once in 3.5 years
    # of data, almost certainly a data entry typo - no realistic citation
    # threshold is that low). Anything below 7 over treated as noise.
    MIN_REALISTIC_DELTA = 7
    cites = [c for c in cites if c["delta"] >= MIN_REALISTIC_DELTA]

    if not cites:
        # Still return volume data even if no speed parses
        return {
            "total": 0,
            "totalTraffic": total_traffic,
            "genericStops": generic_stops,
            "citeBreakdown": cite_breakdown,
            "warnings": warnings_summary,
            "minDelta": 0, "maxDelta": 0, "avgDelta": 0,
            "stacked": 0, "stackedPct": 0,
            "deltaDistribution": [], "byLimit": [], "byCity": [],
            "tailCounts": [], "topAggressive": [], "lowestDeltas": [],
            "recent": [], "allCites": [],
        }

    deltas = [c["delta"] for c in cites]
    min_d = min(deltas)
    max_d = max(deltas)
    avg_d = sum(deltas) / len(deltas)

    buckets = [
        ("5-9 over",   5, 9),
        ("10 over",    10, 10),
        ("11 over",    11, 11),
        ("12 over",    12, 12),
        ("13 over",    13, 13),
        ("14 over",    14, 14),
        ("15-19 over", 15, 19),
        ("20-24 over", 20, 24),
        ("25-29 over", 25, 29),
        ("30+ over",   30, 999),
    ]
    delta_dist = [{"bucket": label,
                   "count": sum(1 for d in deltas if lo <= d <= hi)}
                  for label, lo, hi in buckets]

    by_limit = defaultdict(int)
    for c in cites:
        by_limit[c["limit"]] += 1
    by_limit_list = sorted(
        [{"limit": k, "count": v} for k, v in by_limit.items()],
        key=lambda x: x["limit"]
    )

    by_city_cites = defaultdict(int)
    for c in cites:
        if c["city"] and c["city"].lower() not in ("", "unknown"):
            by_city_cites[c["city"]] += 1
    by_city_list = sorted(
        [{"city": k, "count": v} for k, v in by_city_cites.items()],
        key=lambda x: -x["count"]
    )[:30]

    stacked = 0
    tail_counts = defaultdict(int)
    for c in cites:
        if c["tail"]:
            stacked += 1
            parts = re.split(r"[,/]|\s+AND\s+", c["tail"])
            for p in parts:
                p = p.strip()
                if p and 2 <= len(p) <= 40:
                    tail_counts[p] += 1

    top_aggressive = sorted(
        cites, key=lambda c: (-c["delta"], c["date"])
    )[:25]

    lowest = sorted(cites, key=lambda c: (c["delta"], c["date"]))[:15]

    recent_cites = sorted(cites, key=lambda c: c["date"], reverse=True)[:100]

    # Slim payload for downstream apps - every parsed cite, just the fields needed
    all_cites_slim = [
        {
            "date":     c["date"],
            "city":     c["city"],
            "speed":    c["speed"],
            "limit":    c["limit"],
            "delta":    c["delta"],
            "tail":     c["tail"],
            "location": c["location"],
            "case":     c["case"],
        }
        for c in cites
    ]

    return {
        "total":             len(cites),
        "totalTraffic":      total_traffic,
        "genericStops":      generic_stops,
        "citeBreakdown":     cite_breakdown,
        "warnings":          warnings_summary,
        "minDelta":          min_d,
        "maxDelta":          max_d,
        "avgDelta":          round(avg_d, 1),
        "stacked":           stacked,
        "stackedPct":        round(100.0 * stacked / len(cites), 1),
        "deltaDistribution": delta_dist,
        "byLimit":           by_limit_list,
        "byCity":            by_city_list,
        "tailCounts":        sorted(
            [{"tail": k, "count": v} for k, v in tail_counts.items()],
            key=lambda x: -x["count"]
        )[:15],
        "topAggressive":     top_aggressive,
        "lowestDeltas":      lowest,
        "recent":            recent_cites,
        "allCites":          all_cites_slim,
    }


# Rare event codes that get their own showcase cards. These are dispatch codes
# that happen in Washington County but are almost never in the news.
# Add/remove codes here to change what gets surfaced.
FEATURED_RARE_CODES = [
    "PROSTITUTION",
    "CHILD EXPLOITATION",
    "HUMAN TRAFFICKING",
    "BOMB THREAT",
    "STALKING",
    "HATE CRIME",
    "STRANGULATION",
    "ANIMAL CRUELTY",
]


def _aggregate_one(rows, ctx):
    """Compute aggregates for one slice of rows (global, single city, or single category).
    ctx provides global-window cutoffs so all subsets use the same time slices."""
    if not rows:
        return None

    by_week = defaultdict(lambda: {"total": 0, "categories": defaultdict(int)})
    by_city = defaultdict(int)
    by_category = defaultdict(int)
    by_year = defaultdict(int)
    by_year_category = defaultdict(lambda: defaultdict(int))
    by_hour = [0] * 24
    by_dow = [0] * 7  # Monday = 0, Sunday = 6
    block_count_recent = defaultdict(int)
    block_count_alltime = defaultdict(int)
    other_examples = defaultdict(int)

    first_date = rows[0]["date"]
    last_date  = rows[-1]["date"]

    for row in rows:
        wk = iso_week(row["date"])
        yr = row["date"][:4]
        by_week[wk]["total"] += 1
        by_week[wk]["categories"][row["category"]] += 1
        by_city[row["city"]] += 1
        by_category[row["category"]] += 1
        by_year[yr] += 1
        by_year_category[yr][row["category"]] += 1

        if row.get("hour") is not None:
            by_hour[row["hour"]] += 1

        d = datetime.fromisoformat(row["date"]).date()
        by_dow[d.weekday()] += 1

        if row["category"] == "Other":
            other_examples[row["type"].strip().upper()] += 1

        if row["location"]:
            loc = re.sub(r"^XXX\s*[-–]\s*", "", row["location"])
            loc = re.sub(r"\s+", " ", loc).strip().upper()
            if loc:
                block_count_alltime[(loc, row["city"])] += 1
                if row["date"] >= ctx["block_cutoff"]:
                    block_count_recent[(loc, row["city"])] += 1

    city_list = sorted(
        [(c, n) for c, n in by_city.items() if c and c.lower() not in ("", "unknown")],
        key=lambda x: -x[1]
    )

    week_series = []
    for wk in sorted(by_week.keys()):
        week_series.append({
            "week": wk,
            "total": by_week[wk]["total"],
            "categories": dict(by_week[wk]["categories"]),
        })

    year_series = []
    for yr in sorted(by_year.keys()):
        year_series.append({
            "year": yr,
            "total": by_year[yr],
            "categories": dict(by_year_category[yr]),
        })

    top_blocks = sorted(
        [{"location": loc, "city": city, "count": n}
         for (loc, city), n in block_count_recent.items()],
        key=lambda x: -x["count"]
    )[:25]

    all_time_blocks = sorted(
        [{"location": loc, "city": city, "count": n}
         for (loc, city), n in block_count_alltime.items()],
        key=lambda x: -x["count"]
    )[:25]

    recent = [r for r in rows if r["date"] >= ctx["recent_cutoff"]]
    recent.sort(key=lambda r: (r["date"], r.get("hour") or 0), reverse=True)

    recent30 = [r for r in rows if r["date"] >= ctx["serious_cutoff"]]
    recent30.sort(key=lambda r: (r["date"], r.get("hour") or 0), reverse=True)

    serious_cats_excluding_theft = ctx["serious_cats"] - {"Theft"}
    _shooting_keywords = ("shots fired", "shots heard", "shooting")
    _ofp_keywords = ("ofp violation", "hro violation", "danco", "no contact order",
                     "protection order")
    def _is_serious(r):
        if r["category"] in serious_cats_excluding_theft:
            return True
        t = (r.get("type") or "").lower()
        if r["category"] == "Weapons":
            return any(kw in t for kw in _shooting_keywords)
        if r["category"] == "Threat" and "terroristic" in t:
            return True
        if r["category"] == "Order Violation":
            return any(kw in t for kw in _ofp_keywords)
        if r["category"] == "Harassment" and "stalking" in t:
            return True
        return False
    recent_serious = [r for r in rows
                      if r["date"] >= ctx["serious_cutoff"] and _is_serious(r)]
    recent_serious.sort(key=lambda r: (r["date"], r.get("hour") or 0), reverse=True)

    recent_theft = [r for r in rows
                    if r["date"] >= ctx["serious_cutoff"] and r["category"] == "Theft"]
    recent_theft.sort(key=lambda r: (r["date"], r.get("hour") or 0), reverse=True)

    cat_lookup = {c: n for c, n in by_category.items()}
    tiered = []
    accounted = set()
    for tier_name, cat_names in CATEGORY_TIERS:
        items = []
        for cn in cat_names:
            if cn in cat_lookup:
                items.append({"category": cn, "count": cat_lookup[cn]})
                accounted.add(cn)
        items.sort(key=lambda x: -x["count"])
        tiered.append({"tier": tier_name, "categories": items,
                       "total": sum(i["count"] for i in items)})
    leftover = [{"category": c, "count": n} for c, n in by_category.items()
                if c not in accounted]
    if leftover:
        leftover.sort(key=lambda x: -x["count"])
        tiered.append({"tier": "Other", "categories": leftover,
                       "total": sum(i["count"] for i in leftover)})

    leak_codes_dict = defaultdict(lambda: defaultdict(int))
    for row in rows:
        if row["category"] in ctx["off_ledger_cats"]:
            raw = row["type"].strip().upper()
            if raw:
                leak_codes_dict[row["category"]][raw] += 1
    leak_codes = []
    for tier_name, cat_names in CATEGORY_TIERS:
        if tier_name != "Off The Ledger":
            continue
        for cn in cat_names:
            if cn in leak_codes_dict:
                sorted_codes = sorted(leak_codes_dict[cn].items(), key=lambda x: -x[1])
                leak_codes.append({
                    "category": cn,
                    "codes": [{"code": c, "count": n} for c, n in sorted_codes]
                })

    traffic = aggregate_traffic(rows)

    # Count featured rare codes in this slice (for extra Off The Ledger cards).
    # Uses substring match so minor variants still count.
    featured_rare = {}
    for code in FEATURED_RARE_CODES:
        code_up = code.upper()
        matching = [r for r in rows if code_up in (r.get("type") or "").upper()]
        if matching:
            last = max(r["date"] for r in matching)
            featured_rare[code] = {
                "count": len(matching),
                "last":  last,
            }

    return {
        "firstDate":      first_date,
        "lastDate":       last_date,
        "totalIncidents": len(rows),
        "weekCount":      len(week_series),
        "byWeek":         week_series,
        "byYear":         year_series,
        "byCity":         [{"city": c, "count": n} for c, n in city_list],
        "byCategory":     [{"category": c, "count": n}
                           for c, n in sorted(by_category.items(), key=lambda x: -x[1])],
        "byCategoryTiered": tiered,
        "byHour":         by_hour,
        "byDow":          by_dow,
        "topBlocks":      top_blocks,
        "allTimeBlocks":  all_time_blocks,
        "leakCodes":      leak_codes,
        "featuredRare":   featured_rare,
        "traffic":        traffic,
        "recent":         recent[:400],
        "recent30":       recent30[:300],
        "recentSerious":  recent_serious[:500],
        "recentTheft":    recent_theft[:500],
        "_otherExamples": other_examples,  # internal, stripped before output
    }


def aggregate(all_rows):
    if not all_rows:
        return None

    all_rows.sort(key=lambda r: r["date"])
    last_date  = all_rows[-1]["date"]
    last_dt = datetime.fromisoformat(last_date).date()

    # Pre-compute time-window cutoffs from global last_date so all subsets are aligned
    off_ledger_cats = set()
    serious_cats = set()
    for tier_name, cat_names in CATEGORY_TIERS:
        if tier_name == "Off The Ledger":
            off_ledger_cats.update(cat_names)
        if tier_name in ("Off The Ledger", "Index Crimes"):
            serious_cats.update(cat_names)

    ctx = {
        "last_dt_global":  last_dt,
        "block_cutoff":    (datetime.fromordinal(last_dt.toordinal() - 89)).date().isoformat(),
        "recent_cutoff":   (datetime.fromordinal(last_dt.toordinal() - 6)).date().isoformat(),
        "serious_cutoff":  (datetime.fromordinal(last_dt.toordinal() - 29)).date().isoformat(),
        "off_ledger_cats": off_ledger_cats,
        "serious_cats":    serious_cats,
    }

    # Global aggregate (all time)
    global_agg = _aggregate_one(all_rows, ctx)

    # Print Other diagnostic
    other_examples = global_agg.pop("_otherExamples", {})
    if other_examples:
        log("top 30 uncategorized event types still in 'Other':")
        for et, n in sorted(other_examples.items(), key=lambda x: -x[1])[:30]:
            log(f"   {n:>6}  {et}")

    # Time-window aggregates - pre-compute for each window so the UI can toggle
    log("computing time-window aggregates...")
    windows = {
        "2y":  (datetime.fromordinal(last_dt.toordinal() - 729)).date().isoformat(),
        "1y":  (datetime.fromordinal(last_dt.toordinal() - 364)).date().isoformat(),
        "90d": (datetime.fromordinal(last_dt.toordinal() - 89)).date().isoformat(),
        "30d": (datetime.fromordinal(last_dt.toordinal() - 29)).date().isoformat(),
    }
    window_aggs = {}
    for label, cutoff in windows.items():
        subset = [r for r in all_rows if r["date"] >= cutoff]
        if subset:
            agg = _aggregate_one(subset, ctx)
            agg.pop("_otherExamples", None)
            window_aggs[label] = agg
            log(f"  {label}: {len(subset):,} incidents from {cutoff}")

    # Per-city aggregates (only cities with meaningful volume)
    log("computing per-city aggregates...")
    city_detail = {}
    cities_seen = defaultdict(int)
    for r in all_rows:
        if r["city"] and r["city"].lower() not in ("", "unknown"):
            cities_seen[r["city"]] += 1
    for city, count in cities_seen.items():
        if count < 200:  # skip tiny jurisdictions
            continue
        city_rows = [r for r in all_rows if r["city"] == city]
        city_agg = _aggregate_one(city_rows, ctx)
        city_agg.pop("_otherExamples", None)
        # Also compute per-window aggregates for this city
        city_agg["windows"] = {}
        for label, cutoff in windows.items():
            subset = [r for r in city_rows if r["date"] >= cutoff]
            if subset:
                sub_agg = _aggregate_one(subset, ctx)
                sub_agg.pop("_otherExamples", None)
                city_agg["windows"][label] = sub_agg
        city_detail[city] = city_agg

    # Per-category aggregates (skip Other/Unknown)
    log("computing per-category aggregates...")
    category_detail = {}
    cats_seen = defaultdict(int)
    for r in all_rows:
        cats_seen[r["category"]] += 1
    for cat, count in cats_seen.items():
        if cat in ("Other", "Unknown") or count < 50:
            continue
        cat_rows = [r for r in all_rows if r["category"] == cat]
        cat_agg = _aggregate_one(cat_rows, ctx)
        cat_agg.pop("_otherExamples", None)
        # Per-window aggregates for this category too
        cat_agg["windows"] = {}
        for label, cutoff in windows.items():
            subset = [r for r in cat_rows if r["date"] >= cutoff]
            if subset:
                sub_agg = _aggregate_one(subset, ctx)
                sub_agg.pop("_otherExamples", None)
                cat_agg["windows"][label] = sub_agg
        category_detail[cat] = cat_agg

    # --- Days since last occurrence ---
    # Two flavors: by category (for broader buckets) and by specific event code
    # (for rare but receipts-worthy one-offs like PROSTITUTION or CHILD EXPLOITATION).
    days_since = {}
    days_since_codes = {}

    # Categories worth surfacing for "days since" cards
    days_since_cats = [
        "Kidnapping", "Robbery", "Sex Offense", "Arson",
        "Weapons", "Drugs",
    ]
    for cat in days_since_cats:
        last_occurrence = None
        for r in reversed(all_rows):
            if r["category"] == cat:
                last_occurrence = r["date"]
                break
        if last_occurrence:
            d_last = datetime.fromisoformat(last_occurrence).date()
            days_since[cat] = {
                "last": last_occurrence,
                "days": (last_dt - d_last).days,
            }

    # Specific event codes - the interesting long-tail entries
    # that deserve their own callout when they happen
    days_since_raw_codes = [
        "CHILD EXPLOITATION",
        "INDECENT EXPOSURE",
        "HUMAN TRAFFICKING",
        "BOMB THREAT",
        "STALKING",
        "HATE CRIME",
    ]
    for code in days_since_raw_codes:
        last_occurrence = None
        for r in reversed(all_rows):
            if (r["type"] or "").strip().upper() == code:
                last_occurrence = r["date"]
                break
        if last_occurrence:
            d_last = datetime.fromisoformat(last_occurrence).date()
            days_since_codes[code] = {
                "last": last_occurrence,
                "days": (last_dt - d_last).days,
            }

    # Peak week on record (all time)
    peak_week = None
    for wk_entry in global_agg["byWeek"]:
        if peak_week is None or wk_entry["total"] > peak_week["total"]:
            peak_week = wk_entry
    # Peak week's dominant category
    if peak_week and peak_week.get("categories"):
        top_cat = max(peak_week["categories"].items(), key=lambda x: x[1])
        peak_week = {
            "week":       peak_week["week"],
            "total":      peak_week["total"],
            "topCategory": top_cat[0],
            "topCount":   top_cat[1],
        }

    # Year-over-year: for each category, compute change from previous complete year
    # to the current year's on-pace estimate. The current year is usually partial,
    # so we prorate: (current total / days elapsed) * 365.
    yoy = []
    years = sorted(set(r["date"][:4] for r in all_rows))
    current_year = str(last_dt.year)
    complete_years = [y for y in years if y < current_year]
    if len(complete_years) >= 2:
        yr_prev, yr_curr = complete_years[-2], complete_years[-1]
    elif len(years) >= 2:
        yr_prev, yr_curr = years[-2], years[-1]
    else:
        yr_prev, yr_curr = None, None
    if yr_prev and yr_curr:
        prev_cat_counts = defaultdict(int)
        curr_cat_counts = defaultdict(int)

        curr_year_int = int(yr_curr)
        year_start = datetime(curr_year_int, 1, 1).date()
        year_end = datetime(curr_year_int, 12, 31).date()
        if yr_curr < current_year:
            # Complete year — use actual days in that year (365 or 366)
            days_elapsed = (year_end - year_start).days + 1
        else:
            days_elapsed = max(1, (last_dt - year_start).days + 1)
        pace_factor = ((year_end - year_start).days + 1) / days_elapsed

        for r in all_rows:
            yr = r["date"][:4]
            if yr == yr_prev:
                prev_cat_counts[r["category"]] += 1
            elif yr == yr_curr:
                curr_cat_counts[r["category"]] += 1

        all_cats = set(prev_cat_counts) | set(curr_cat_counts)
        for cat in all_cats:
            if cat in ("Other", "Unknown"):
                continue
            prev_n = prev_cat_counts.get(cat, 0)
            curr_n = curr_cat_counts.get(cat, 0)
            curr_on_pace = round(curr_n * pace_factor)
            # Require at least 50 in prev year to avoid noise on rare categories
            if prev_n < 50:
                continue
            pct = ((curr_on_pace - prev_n) / prev_n) * 100
            yoy.append({
                "category":  cat,
                "prevYear":  yr_prev,
                "currYear":  yr_curr,
                "prev":      prev_n,
                "curr":      curr_n,
                "currOnPace": curr_on_pace,
                "delta":     curr_on_pace - prev_n,
                "pct":       round(pct, 1),
                "daysElapsed": days_elapsed,
            })
        yoy.sort(key=lambda x: -abs(x["pct"]))

    # Weekend vs weekday split overall
    weekend_count = global_agg["byDow"][5] + global_agg["byDow"][6]  # Sat + Sun
    weekday_count = sum(global_agg["byDow"][0:5])
    weekend_split = {
        "weekend": weekend_count,
        "weekday": weekday_count,
        "weekendPct": round(100.0 * weekend_count / max(1, weekend_count + weekday_count), 1),
    }

    # Peak hour overall
    peak_hour = None
    for i, n in enumerate(global_agg["byHour"]):
        if peak_hour is None or n > peak_hour[1]:
            peak_hour = (i, n)

    # Peak day of week overall
    dow_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    peak_dow_idx = 0
    for i, n in enumerate(global_agg["byDow"]):
        if n > global_agg["byDow"][peak_dow_idx]:
            peak_dow_idx = i

    notable = {
        "daysSince":      days_since,
        "daysSinceCodes": days_since_codes,
        "peakWeek":     peak_week,
        "yoy":          yoy,
        "weekendSplit": weekend_split,
        "peakHour":     {"hour": peak_hour[0], "count": peak_hour[1]} if peak_hour else None,
        "peakDow":      {"name": dow_names[peak_dow_idx], "count": global_agg["byDow"][peak_dow_idx]},
    }

    return {
        "meta": {
            "generated":      datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "firstDate":      global_agg["firstDate"],
            "lastDate":       global_agg["lastDate"],
            "totalIncidents": global_agg["totalIncidents"],
            "weekCount":      global_agg["weekCount"],
            "source":         "Washington County Sheriff RMS (open data)",
            "url":            "https://www.washingtoncountymn.gov/2979/Law-Enforcement-Public-Data",
        },
        # Also expose at top level so the JS view-picker works uniformly for global vs detail
        "firstDate":          global_agg["firstDate"],
        "lastDate":           global_agg["lastDate"],
        "totalIncidents":     global_agg["totalIncidents"],
        "weekCount":          global_agg["weekCount"],
        # Global view (default)
        "byWeek":             global_agg["byWeek"],
        "byYear":             global_agg["byYear"],
        "byCity":             global_agg["byCity"],
        "byCategory":         global_agg["byCategory"],
        "byCategoryTiered":   global_agg["byCategoryTiered"],
        "byHour":             global_agg["byHour"],
        "topBlocks":          global_agg["topBlocks"],
        "leakCodes":          global_agg["leakCodes"],
        "traffic":            global_agg["traffic"],
        "recent":             global_agg["recent"],
        "recentSerious":      global_agg["recentSerious"],
        "recentTheft":        global_agg["recentTheft"],
        # Drill-down views
        "cityDetail":         city_detail,
        "categoryDetail":     category_detail,
        # Pre-computed time-window aggregates for the top-of-page toggle
        "windows":            window_aggs,
        # Notable numbers (all-time observations)
        "notable":            notable,
    }


def write_data_js(payload):
    body = json.dumps(payload, separators=(",", ":"))
    DATA_JS.write_text(f"window.CRIME_DATA = {body};\n", encoding="utf-8")
    log(f"wrote {DATA_JS} ({DATA_JS.stat().st_size/1024:.1f} KB)")


ROWS_JS = HERE / "rows.js"


def write_rows_js(all_rows):
    """Write rows.js: compact row-level data for client-side filtering.

    Format:
        window.CRIME_ROWS = {
            cities:     ["Bayport", "Cottage Grove", ...],
            categories: ["Theft", "Vehicle Crime", ...],
            types:      ["TRAFFIC STOP", "MEDICAL ASSIST", ...],  // by frequency
            tiers:      {"Off The Ledger": ["Suicide / Suicidal", ...], ...},
            agencies:   {"Bayport": "651-275-4404", ...},  // contact lookup
            rows: [
                [yyyymmdd, hour, cityIdx, catIdx, typeIdx, location, case],
                ...
            ]
        }
    """
    if not all_rows:
        return

    # Lookup tables
    cities = sorted({r["city"] for r in all_rows
                     if r["city"] and r["city"].lower() != "unknown"})
    city_idx = {c: i for i, c in enumerate(cities)}

    # Categories preserved in tier order (so client can group easily)
    categories = []
    for _, cat_names in CATEGORY_TIERS:
        for cn in cat_names:
            if cn not in categories:
                categories.append(cn)
    for r in all_rows:
        if r["category"] not in categories:
            categories.append(r["category"])
    cat_idx = {c: i for i, c in enumerate(categories)}

    # Types interned by frequency (most common first → smaller indexes)
    type_counts = defaultdict(int)
    for r in all_rows:
        type_counts[r["type"]] += 1
    types = sorted(type_counts.keys(), key=lambda t: -type_counts[t])
    type_idx = {t: i for i, t in enumerate(types)}

    # Agency contacts (from Washington County records page)
    agencies = {
        "Bayport":          "651-275-4404",
        "Cottage Grove":    "651-458-2850",
        "Forest Lake":      "651-464-5877",
        "Oak Park Heights": "651-439-4723",
        "Oakdale":          "651-738-1025",
        "Saint Paul Park":  "651-459-9785",
        "Stillwater":       "651-351-4900",
        "Woodbury":         "651-714-3600",
        "_default":         "651-430-7600",  # WCSO covers everything else
    }

    # Compact rows
    compact = []
    for r in all_rows:
        if r["city"] not in city_idx:
            continue  # skip rows we couldn't normalize a city for
        date_int = int(r["date"].replace("-", ""))
        hour = r["hour"] if r["hour"] is not None else -1
        compact.append([
            date_int,
            hour,
            city_idx[r["city"]],
            cat_idx[r["category"]],
            type_idx[r["type"]],
            r["location"] or "",
            r["case"] or "",
        ])

    tiers = {tn: list(cn) for tn, cn in CATEGORY_TIERS}

    out = {
        "cities":     cities,
        "categories": categories,
        "types":      types,
        "tiers":      tiers,
        "agencies":   agencies,
        "rows":       compact,
    }
    body = json.dumps(out, separators=(",", ":"))
    ROWS_JS.write_text(f"window.CRIME_ROWS = {body};\n", encoding="utf-8")
    log(f"wrote {ROWS_JS} ({ROWS_JS.stat().st_size/1024:.1f} KB, {len(compact):,} rows)")


def main():
    CACHE_DIR.mkdir(exist_ok=True)

    try:
        remote = list_remote_files()
    except Exception as e:
        log(f"could not list remote files: {e}")
        sys.exit(1)

    log(f"downloading (cached files will be skipped)...")
    new_count = 0
    for i, name in enumerate(remote, 1):
        _, was_new = download(name)
        if was_new:
            new_count += 1
            time.sleep(0.4)  # be polite
        if i % 25 == 0 or i == len(remote):
            log(f"  {i}/{len(remote)} ({new_count} new this run)")

    log("parsing all cached files...")
    all_rows = []
    for path in sorted(CACHE_DIR.glob("IncidentSummary_*.csv")):
        all_rows.extend(parse_csv(path))
    log(f"parsed {len(all_rows):,} incident rows across {sum(1 for _ in CACHE_DIR.glob('IncidentSummary_*.csv'))} files")

    payload = aggregate(all_rows)
    if payload is None:
        log("no data to write")
        sys.exit(1)

    write_data_js(payload)
    write_rows_js(all_rows)
    log(f"done: {payload['meta']['totalIncidents']:,} incidents from {payload['meta']['firstDate']} to {payload['meta']['lastDate']}")


if __name__ == "__main__":
    main()
