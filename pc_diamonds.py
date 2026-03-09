import traceback
import csv
import os
import requests
from dotenv import load_dotenv
from datetime import datetime
from logger import get_logger

load_dotenv()

log = get_logger("pc_diamonds")

base_dir   = os.getcwd()
down_files = os.path.join(base_dir, "diamond_files")
os.makedirs(down_files, exist_ok=True)

OUTPUT_FILES = os.path.join(down_files, "pc_diamonds.csv")


def get_token():
    """Fetch authentication token from the API."""
    try:
        base_url = f"{os.getenv('URL')}?apikey={os.getenv('APIKEY')}"
        response = requests.post(base_url)
        data = response.json()

        if response.status_code != 200:
            log.error(f"Token request failed (HTTP {response.status_code}): {data.get('error', 'Unknown error')}")
            return None

        token = data.get("token")
        if not token:
            log.error("Token not found in API response.")
            return None

        log.info("Token retrieved successfully.")
        return token

    except Exception as e:
        log.exception(f"Exception while fetching token: {e}")
        return None


def collect_diamond_data(csv_file_fullpath):
    """Use token to collect diamond data and save to a CSV file."""
    start_time = datetime.now()
    log.info("=" * 50)
    log.info("Starting PC Diamonds data collection")
    log.info(f"Output file: {csv_file_fullpath}")

    # --- Step 1: Get Token ---
    token = get_token()
    if not token:
        log.error("Failed to retrieve token. Aborting data collection.")
        return {
            'action': "Import", 'api_resp': "error",
            'error_msg': "Failed to retrieve token.",
            'diamond_entry': "Diamondport api",
            'start_time': start_time, 'end_time': datetime.now()
        }

    # --- Step 2: Collect Diamond Data ---
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + str(token)
    }
    payload = {
        'shape': 'Round,Radiant,Princess,Pear,Oval,Marquise,Heart,Emerald,Cushion,Asscher',
        'color': 'D,E,F,G,H,I,J,H,I,J,K',
        'clarity': 'FL,IF,VVS1,VVS2,VS1,VS2,SI1',
        'carat': '0.50-30',
        'cut': 'EX,VG,GD,ID',
        'polish': 'EX,VG,GD,FR',
        'symmetry': 'EX,VG,GD,FR',
        'fluorescence': 'NON,FNT,MED,STG,VST',
        'lab': 'IGI,GIA,HRD,AGS',
        'availability': 'available',
        'image_video': 3,
        'perpage': '10000'
    }

    diamonds_list = []
    api_link      = os.getenv('API_LINK')
    is_allow_loop = True
    err_msg       = ""
    response      = None
    page_num      = 0

    while is_allow_loop:
        page_num += 1
        log.debug(f"Fetching page {page_num} from: {api_link}")
        try:
            response = requests.post(api_link, headers=headers, data=payload)
        except Exception as e:
            response  = None
            err_msg   = str(e)
            log.error(f"API request failed on page {page_num}: {err_msg}")
            is_allow_loop = False
            break

        if response is None or response.status_code != 200:
            err_msg = f"API returned status {response.status_code if response else 'No response'}"
            log.error(err_msg)
            is_allow_loop = False
            break

        try:
            json_data = response.json()
        except Exception as e:
            err_msg = "Failed to parse JSON from API response."
            log.error(f"{err_msg}: {e}")
            is_allow_loop = False
            break

        if not json_data:
            log.warning("Empty JSON response received — stopping loop.")
            is_allow_loop = False
            break

        diamonds_data = json_data.get('data', [])
        next_page_url = json_data.get('nextPageUrl', "")

        if diamonds_data:
            diamonds_list.extend(diamonds_data)
            log.info(f"Page {page_num}: +{len(diamonds_data)} diamonds | Total so far: {len(diamonds_list):,}")

        if next_page_url and str(next_page_url).strip() not in ("", "None"):
            api_link = str(next_page_url).strip()
        else:
            log.info("No next page URL — all pages fetched.")
            is_allow_loop = False

    end_time = datetime.now()

    # --- Step 3: Handle No Response ---
    if response is None:
        log.error("No API response received at all — possible connection issue.")
        return {
            'action': "Import", 'api_resp': "error",
            'error_msg': "Diamondport API connection problem",
            'diamond_entry': "Diamondport api",
            'start_time': start_time, 'end_time': end_time
        }

    # --- Step 4: Write to CSV ---
    is_file_saved = False
    api_response  = "error"

    try:
        if diamonds_list:
            csv_columns = list(diamonds_list[0].keys())
            with open(csv_file_fullpath, 'w+', newline="") as csvFile:
                writer = csv.writer(csvFile)
                writer.writerow(csv_columns)
                for row in diamonds_list:
                    writer.writerow([row.get(col, "") for col in csv_columns])

            log.info(f"CSV saved: {csv_file_fullpath} ({len(diamonds_list):,} records)")
            is_file_saved = True
            api_response  = "ok"
        else:
            err_msg = "No diamond data returned from API."
            log.warning(err_msg)

    except Exception as e:
        err_msg = str(e)
        log.exception(f"Failed to write CSV: {err_msg}")

    end_time = datetime.now()
    elapsed  = (end_time - start_time).total_seconds()
    log.info(f"Collection finished in {elapsed:.1f}s | status: {api_response}")
    log.info("=" * 50)

    return {
        'action': "Import",
        'api_resp': api_response,
        'error_msg': err_msg if not is_file_saved else "",
        'diamond_entry': "Diamondport api",
        'start_time': start_time,
        'end_time': end_time
    }


if __name__ == "__main__":
    result = collect_diamond_data(OUTPUT_FILES)