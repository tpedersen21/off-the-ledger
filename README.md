# Washington County Incident Ledger

A clean HTML dashboard over Washington County's open incident data, covering roughly three and a half years of weekly RMS reports from Bayport, Cottage Grove, Forest Lake, Oak Park Heights, Oakdale, Saint Paul Park, Stillwater, Woodbury, and Sheriff-covered areas.

## Quick start

Open Terminal, cd into this folder, then:

TERMINAL: `./update.sh`

First run pulls ~180 weekly CSVs from the county server and builds `data.js`. Takes a few minutes. Subsequent runs only grab new weekly files and are near-instant.

After the script finishes, `index.html` will open in your default browser.

## Files

- `fetch_data.py` — downloads, caches, combines, and exports `data.js`. Pure Python 3, no pip install required.
- `index.html` — the dashboard. Reads `data.js` on load. Opens fine from the filesystem (no server needed).
- `update.sh` — convenience wrapper. Runs the fetcher then opens the page.
- `cache/` — the raw CSVs, kept around so subsequent runs are fast and so you can do one-off analysis on the source files.
- `data.js` — generated aggregate data. Committed by the fetcher, read by the page.

## What's in the data

Participating agencies report fourteen incident categories: theft, motor vehicle theft, burglary, assault, robbery, vandalism, DUI, drugs/alcohol, fraud, weapons, arson, homicide, disturbance, and vehicle break-in.

## What's not in the data

The county strips these categories from the public feed before publication, which means they can't be seen in this ledger:

- Domestic assault (physical and verbal)
- Criminal sexual conduct
- Child or elder abuse
- Mental health crisis / evaluation
- Emotionally disturbed person
- Search warrants
- Extra patrol requests
- Background checks

Any pattern story about those topics has to come from MCRO court records (`publicaccess.courts.state.mn.us`) and BCA/Sheriff press releases, not this dataset.

## Notes

- Data is pulled from `https://web1.co.washington.mn.us/MediaReports/RMS/`. The county's public docs say four-week rolling, but their directory actually holds everything since September 2022.
- All counts are "as reported by the originating agency." The county's own disclaimer: illustrative, not an official crime report, subject to change.
- If the fetcher fails on a file, it logs and moves on. Re-running picks up any it missed.
