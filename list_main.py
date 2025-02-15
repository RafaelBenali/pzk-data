import os
import re
import csv
import json
import time
import shutil
import glob
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs
from rapidfuzz import process, fuzz
from dateutil import parser
from flask import Flask, request, jsonify, send_from_directory

# -----------------------------------------------------------------------------
# Persistent disk configuration
# -----------------------------------------------------------------------------
PERSISTENT_DIR = "/var/map"
if not os.path.exists(PERSISTENT_DIR):
    os.makedirs(PERSISTENT_DIR)

# Files stored on persistent disk
LOG_FILE = os.path.join(PERSISTENT_DIR, "pipeline.log")
IMAGES_DIR = os.path.join(PERSISTENT_DIR, "images")
if not os.path.exists(IMAGES_DIR):
    os.makedirs(IMAGES_DIR)

REFERENCE_GEOJSON_FILE = os.path.join(PERSISTENT_DIR, "ospace.addresses.geojson")

# -----------------------------------------------------------------------------
# GLOBAL CONFIG / LOGGING / GLOBAL COUNTERS
# -----------------------------------------------------------------------------
URL_1 = "https://memopzk.org/list-persecuted/spisok-politzaklyuchyonnyh-presleduemyh-za-religiyu/?download"
URL_2 = "https://memopzk.org/list-persecuted/spisok-politzaklyuchyonnyh-bez-presleduemyh-za-religiyu/?download"

FALLBACK_COORDS = [96.712933, 62.517018]
FUZZY_THRESHOLD = 70
INTERACTIVE_MODE = False

ERROR_COUNT = 0  # Global error counter

logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def log_error(message):
    """Log error and increment error counter."""
    global ERROR_COUNT
    ERROR_COUNT += 1
    logger.error(message)


# -----------------------------------------------------------------------------
# Helper: robust_get() with retries and exponential backoff
# -----------------------------------------------------------------------------
def robust_get(url, max_retries=3, initial_delay=1, **kwargs):
    attempt = 0
    delay = initial_delay
    while attempt < max_retries:
        try:
            response = requests.get(url, **kwargs)
            response.raise_for_status()
            return response
        except Exception as e:
            attempt += 1
            if attempt >= max_retries:
                raise e
            time.sleep(delay)
            delay *= 2

# -----------------------------------------------------------------------------
# 1) DOWNLOAD & MERGE CSVs
# -----------------------------------------------------------------------------
def download_file(url):
    try:
        resp = robust_get(url, timeout=15)
    except Exception as e:
        log_error(f"Failed to download from {url}: {e}")
        return None

    cd_header = resp.headers.get("Content-Disposition", "")
    filename = "downloaded_file.csv"
    match = re.search(r'filename="?([^"]+)"?', cd_header)
    if match:
        filename = match.group(1)

    with open(filename, 'wb') as f:
        f.write(resp.content)
    logger.info(f"Downloaded: {filename}")
    return filename

def extract_date_from_filename(fname):
    match = re.search(r'list_\d+_(\d{2}-\d{2}-\d{4})\.csv', fname)
    if match:
        return match.group(1)
    return None

def merge_two_csvs(file1, file2):
    df1 = pd.read_csv(file1, skiprows=1, delimiter=';', header=None)
    df2 = pd.read_csv(file2, skiprows=1, delimiter=';', header=None)
    df1.dropna(how="all", inplace=True)
    df2.dropna(how="all", inplace=True)
    df1.columns = ['Column_1', 'Column_2']
    df2.columns = ['Column_1', 'Column_2']
    combined_df = pd.concat([df1, df2], ignore_index=True)
    date_str = extract_date_from_filename(file1)
    if not date_str:
        date_str = datetime.now().strftime('%d-%m-%Y')
    return combined_df, date_str

def create_merged_csv():
    file1 = download_file(URL_1)
    time.sleep(15)  # pause increased to 15s
    file2 = download_file(URL_2)
    if not file1 or not file2:
        log_error("Error downloading one or both CSV files.")
        return None
    merged_df, date_str = merge_two_csvs(file1, file2)
    rowcount = len(merged_df)
    final_csv_name = f"list_{rowcount}_{date_str}.csv"
    merged_df.to_csv(final_csv_name, index=False, header=False, sep=';')
    logger.info(f"Merged CSV created: {final_csv_name}")
    for f in [file1, file2]:
        try:
            os.remove(f)
        except Exception as e:
            log_error(f"Could not remove file {f}: {e}")
    return final_csv_name

# -----------------------------------------------------------------------------
# 2) GET/UPDATE EXISTING GEOJSON
# -----------------------------------------------------------------------------
def parse_geojson_date(fname):
    match = re.match(r'^list_\d+_(\d{2}-\d{2}-\d{4})\.geojson$', fname)
    if not match:
        return None
    datestr = match.group(1)
    try:
        return datetime.strptime(datestr, '%d-%m-%Y')
    except:
        return None

def get_latest_geojson():
    files = [f for f in os.listdir(PERSISTENT_DIR) if f.startswith("list_") and f.endswith(".geojson")]
    valid = []
    for f in files:
        dt = parse_geojson_date(f)
        if dt:
            valid.append((f, dt))
    if not valid:
        return None
    latest = max(valid, key=lambda x: x[1])[0]
    return os.path.join(PERSISTENT_DIR, latest)

def load_geojson(fname):
    if not fname or not os.path.exists(fname):
        return {"type": "FeatureCollection", "features": []}
    try:
        with open(fname, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        log_error(f"Error loading geojson '{fname}': {e}")
        return {"type": "FeatureCollection", "features": []}

def save_geojson(data, fname):
    try:
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved GeoJSON => {fname}")
    except Exception as e:
        log_error(f"Error saving geojson '{fname}': {e}")

def extract_ids_from_geojson(geojson_data):
    feats = geojson_data.get("features", [])
    out = set()
    for feat in feats:
        props = feat.get("properties", {})
        if "ID" in props:
            out.add(str(props["ID"]))
    return out

# -----------------------------------------------------------------------------
# 3) EXTRACT & COMPARE CSV vs GEOJSON
# -----------------------------------------------------------------------------
def extract_ids_from_csv(csv_file):
    df = pd.read_csv(csv_file, delimiter=';', header=None)
    df.dropna(how='all', inplace=True)
    all_ids = set()
    for _, row in df.iterrows():
        if len(row) < 2:
            continue
        url = str(row[1]).strip()
        m = re.search(r'\?p=(\d+)', url)
        if m:
            all_ids.add(m.group(1))
    return all_ids

def diff_ids(csv_ids, geojson_ids):
    added = csv_ids - geojson_ids
    removed = geojson_ids - csv_ids
    return added, removed

# -----------------------------------------------------------------------------
# Helper: FETCH UPDATED LINK FROM WP JSON API
# -----------------------------------------------------------------------------
def fetch_updated_link(page_id):
    json_url = f"https://memopzk.org/wp-json/wp/v2/figurant/{page_id}"
    try:
        resp = robust_get(json_url, timeout=15)
        data = resp.json()
        updated_link = data.get("link", "")
        if updated_link:
            logger.info(f"[UPDATE_LINK] ID={page_id} updated link: {updated_link}")
        else:
            log_error(f"[UPDATE_LINK] ID={page_id} did not return a 'link'. Using fallback URL.")
            updated_link = f"https://memopzk.org/?p={page_id}"
    except Exception as e:
        log_error(f"[UPDATE_LINK] Error fetching updated link for ID={page_id}: {e}")
        updated_link = f"https://memopzk.org/?p={page_id}"
    time.sleep(15)
    return updated_link

# -----------------------------------------------------------------------------
# 4) SCRAPE + CREATE TEMP GEOJSON for new IDs
# -----------------------------------------------------------------------------
def ensure_images_dir():
    if not os.path.exists(IMAGES_DIR):
        os.makedirs(IMAGES_DIR)

def build_id_url_map(csv_file):
    df = pd.read_csv(csv_file, delimiter=';', header=None)
    df.dropna(how='all', inplace=True)
    id_map = {}
    for _, row in df.iterrows():
        if len(row) < 2:
            continue
        url = str(row[1]).strip()
        m = re.search(r'\?p=(\d+)', url)
        if m:
            the_id = m.group(1)
            id_map[the_id] = url
    return id_map

def scrape_one_id(page_id, url):
    record = {
        "ID": page_id,
        "sourceUrl": url,
        "geocodeStatus": "pending"
    }
    try:
        resp = robust_get(url, timeout=10)
    except Exception as e:
        log_error(f"[SCRAPE] Request error for ID={page_id}: {e}")
        record["name"] = "N/A"
        return record

    soup = BeautifulSoup(resp.text, 'html.parser')
    # 1) Name
    name_div = soup.find('div', class_='human-dossier__name')
    if name_div:
        h1 = name_div.find('h1', class_='title title--lg')
        record["name"] = h1.get_text(strip=True) if h1 else "N/A"
    else:
        record["name"] = "N/A"
    # 2) Clauses
    clauses = []
    clause_list = soup.find('ul', class_='clause__list')
    if clause_list:
        items = clause_list.find_all('li', class_='clause__item')
        for it in items:
            sp = it.find('span')
            if sp:
                clauses.append(sp.get_text(strip=True))
    record["clauses"] = clauses
    # 3) Main paragraphs – DEDUPLICATED
    main_paragraphs = []
    dossier_art = soup.find('div', class_='human-dossier__art')
    if dossier_art:
        for p_tag in dossier_art.find_all('p'):
            p_text = p_tag.get_text(" ", strip=True)
            if p_text and p_text not in main_paragraphs:
                main_paragraphs.append(p_text)
    if main_paragraphs:
        composite_main = "\n".join(f"<p>{p}</p>" for p in main_paragraphs)
        record["main"] = [composite_main]
    else:
        record["main"] = []
    # 4) Tags
    tags = []
    dossier_list = soup.find('ul', class_='dossier-info__list')
    if dossier_list:
        a_tags = dossier_list.find_all('a', class_='dossier-info__link')
        for a_tag in a_tags:
            label = a_tag.get('aria-label')
            if label:
                tags.append(label)
    record["tags"] = tags
    # 5) Address – DEDUPLICATED
    address_pars = []
    modal_div = soup.find('div', attrs={'data-modal': 'letter'})
    if modal_div:
        modal_content = modal_div.find('div', class_='modal-content')
        if modal_content:
            for p_tag in modal_content.find_all('p'):
                p_text = p_tag.get_text(" ", strip=True)
                if p_text and p_text not in address_pars:
                    address_pars.append(p_text)
    if address_pars:
        composite_address = "\n".join(f"<p>{p}</p>" for p in address_pars)
        record["address"] = [composite_address]
    else:
        record["address"] = []
    # 6) Blog link
    linkink = ""
    dossier_card = soup.find('div', class_='human-dossier-card')
    if dossier_card:
        blog_a = dossier_card.find(
            'a',
            class_='btn btn--blue-border human-dossier-card__btn-email',
            attrs={'aria-label': 'Блог фигуранта'}
        )
        if blog_a:
            linkink = blog_a.get('href', '').strip()
    record["linkink"] = linkink
    # 7) Image
    record["imageUrl"] = ""
    record["imageFilename"] = ""
    image_div = soup.find('div', class_='human-dossier-card__img')
    if image_div:
        img_tag = image_div.find('img')
        if img_tag and img_tag.get('src'):
            record["imageUrl"] = img_tag['src'].strip()
        else:
            style_attr = image_div.get('style', '')
            m2 = re.search(r"url\(['\"]?(.*?)['\"]?\)", style_attr)
            if m2:
                record["imageUrl"] = m2.group(1).strip()
    if record["imageUrl"]:
        ext = os.path.splitext(record["imageUrl"])[1].split('?')[0]
        local_fname = f"{page_id}{ext}"
        local_path = os.path.join(IMAGES_DIR, local_fname)
        try:
            r_img = robust_get(record["imageUrl"], stream=True, timeout=10)
            with open(local_path, 'wb') as f_out:
                for chunk in r_img.iter_content(1024):
                    f_out.write(chunk)
            record["imageFilename"] = local_fname
        except Exception as e:
            log_error(f"[SCRAPE] Error downloading image for ID={page_id}: {e}")
    return record

def scrape_new_ids(new_ids, csv_file):
    ensure_images_dir()
    id_map = build_id_url_map(csv_file)
    new_features = []
    for i, page_id in enumerate(sorted(new_ids), start=1):
        try:
            updated_link = fetch_updated_link(page_id)
            record = scrape_one_id(page_id, updated_link)
        except Exception as e:
            log_error(f"[SCRAPE] Exception for ID={page_id}: {e}")
            record = {"ID": page_id, "sourceUrl": id_map.get(page_id, ""), "geocodeStatus": f"Error: {e}"}
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": record
        }
        new_features.append(feature)
        logger.info(f"[SCRAPE] ID={page_id} done ({i}/{len(new_ids)})")
        time.sleep(15)
    return new_features

# -----------------------------------------------------------------------------
# 5) PARTIAL GEOCODING (just for new features)
# -----------------------------------------------------------------------------
def load_reference_geojson(ref_geojson):
    if not os.path.exists(ref_geojson):
        logger.warning(f"No reference geojson found: {ref_geojson}")
        return {}
    try:
        with open(ref_geojson, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        log_error(f"Error loading reference geojson: {e}")
        return {}
    out = {}
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        geo_pc = props.get("postcode", "").strip()
        txt_parts = []
        for k in ["address", "name", "region"]:
            if props.get(k):
                txt_parts.append(str(props[k]))
        combined = " | ".join(txt_parts)
        if geo_pc not in out:
            out[geo_pc] = []
        out[geo_pc].append((feat, combined))
    return out

def fuzzy_match(candidate, choices):
    match_res = process.extractOne(candidate, choices, scorer=fuzz.token_sort_ratio)
    if match_res:
        return match_res[0], match_res[1]
    return None, 0

def geocode_new_features(features, ref_dict):
    for feat in features:
        try:
            props = feat.get("properties", {})
            page_id = props.get("ID", "")
            rec_postcode = props.get("postcode", "").strip() if "postcode" in props else ""
            chosen_coords = FALLBACK_COORDS[:]
            geo_status = "pending"
            address_field = props.get("address", [])
            candidate_text = " ".join(address_field) if isinstance(address_field, list) else str(address_field)
            if not rec_postcode:
                geo_status = "rf"
                chosen_coords = FALLBACK_COORDS
                logger.info(f"[GEO] ID={page_id} => No postcode => geocodeStatus=rf => coords={chosen_coords}")
            else:
                if rec_postcode not in ref_dict:
                    geo_status = "Индекс не найден"
                    chosen_coords = FALLBACK_COORDS
                    logger.info(f"[GEO] ID={page_id} => Postcode '{rec_postcode}' not in reference => coords={chosen_coords}")
                else:
                    candidates = ref_dict[rec_postcode]
                    if len(candidates) == 1:
                        cfeat, ctxt = candidates[0]
                        coords = cfeat.get("geometry", {}).get("coordinates", [])
                        if coords and len(coords) == 2:
                            chosen_coords = coords
                            geo_status = "True"
                            logger.info(f"[GEO] ID={page_id} => Single match => coords={chosen_coords} => geocodeStatus=True")
                        else:
                            geo_status = "Индекс не найден"
                            logger.info(f"[GEO] ID={page_id} => Single match but no valid coords => fallback => geocodeStatus={geo_status}")
                    else:
                        if INTERACTIVE_MODE:
                            print(f"\n-- Manual Check for ID={page_id} --")
                            print(f"Original Address: {candidate_text}")
                            for i, (candidate_feat, candidate_text_candidate) in enumerate(candidates):
                                this_score = fuzz.token_sort_ratio(candidate_text, candidate_text_candidate)
                                print(f"  [{i}] Score={this_score} => {candidate_text_candidate}")
                            user_input = input("Choose index [or press Enter to skip]: ").strip()
                            if user_input.isdigit():
                                pick_idx = int(user_input)
                                if 0 <= pick_idx < len(candidates):
                                    pick_feat = candidates[pick_idx][0]
                                    coords = pick_feat.get("geometry", {}).get("coordinates", [])
                                    if coords and len(coords) == 2:
                                        chosen_coords = coords
                                        geo_status = "True"
                                        print(f"Chosen index: {pick_idx}, coords={coords}, status=True")
                                        logger.info(f"[GEO] ID={page_id} => user-chosen index={pick_idx} => coords={coords}, geocodeStatus=True")
                                    else:
                                        print("No coords in that feature, fallback used.")
                                        logger.info(f"[GEO] ID={page_id} => user-chosen index={pick_idx}, but no coords => fallback used")
                                else:
                                    print("Invalid index, fallback used.")
                            else:
                                print("No index chosen, fallback used.")
                            if geo_status != "True":
                                best_str, best_score = fuzzy_match(candidate_text, [x[1] for x in candidates])
                                if best_str and best_score >= FUZZY_THRESHOLD:
                                    chosen_ref = next((rfeat for (rfeat, rtxt) in candidates if rtxt == best_str), None)
                                    if chosen_ref:
                                        coords = chosen_ref.get("geometry", {}).get("coordinates", [])
                                        if coords and len(coords) == 2:
                                            chosen_coords = coords
                                            geo_status = "True"
                                            logger.info(f"[GEO] ID={page_id} => Fuzzy matched coords={chosen_coords} => geocodeStatus=True")
                                        else:
                                            geo_status = "Индекс не найден"
                                            logger.info(f"[GEO] ID={page_id} => Fuzzy matched but coords missing => fallback => geocodeStatus={geo_status}")
                                    else:
                                        geo_status = "Требуется проверка"
                                        logger.info(f"[GEO] ID={page_id} => Could not find feature for bestStr => geocodeStatus={geo_status}")
                                else:
                                    geo_status = "Требуется проверка"
                                    logger.info(f"[GEO] ID={page_id} => Fuzzy score < {FUZZY_THRESHOLD} => fallback => geocodeStatus={geo_status}")
                        else:
                            best_str, best_score = fuzzy_match(candidate_text, [x[1] for x in candidates])
                            logger.info(f"[GEO] ID={page_id} => Multiple candidates => bestStr='{best_str}', score={best_score}")
                            if best_str and best_score >= FUZZY_THRESHOLD:
                                chosen_ref = next((rfeat for (rfeat, rtxt) in candidates if rtxt == best_str), None)
                                if chosen_ref:
                                    coords = chosen_ref.get("geometry", {}).get("coordinates", [])
                                    if coords and len(coords) == 2:
                                        chosen_coords = coords
                                        geo_status = "True"
                                        logger.info(f"[GEO] ID={page_id} => Fuzzy matched coords={chosen_coords} => geocodeStatus=True")
                                    else:
                                        geo_status = "Индекс не найден"
                                        logger.info(f"[GEO] ID={page_id} => Fuzzy matched but coords missing => fallback => geocodeStatus={geo_status}")
                                else:
                                    geo_status = "Требуется проверка"
                                    logger.info(f"[GEO] ID={page_id} => Could not find feature for bestStr => geocodeStatus={geo_status}")
                            else:
                                geo_status = "Требуется проверка"
                                logger.info(f"[GEO] ID={page_id} => Fuzzy score < {FUZZY_THRESHOLD} => fallback => geocodeStatus={geo_status}")
            feat["geometry"]["coordinates"] = chosen_coords
            props["geocodeStatus"] = geo_status
            feat["properties"] = props
        except Exception as e:
            log_error(f"[GEO] Error processing ID={props.get('ID', '')}: {e}")
            props["geocodeStatus"] = f"Error: {e}"
            feat["properties"] = props
        # End try/except for this feature
    return features

# -----------------------------------------------------------------------------
# 6) DATE CLEANING (plus POSTCODE extraction) -- applied only to new features
# -----------------------------------------------------------------------------
RUSSIAN_MONTHS = {
    'января': '01',
    'февраля': '02',
    'марта': '03',
    'апреля': '04',
    'мая': '05',
    'июня': '06',
    'июля': '07',
    'августа': '08',
    'сентября': '09',
    'октября': '10',
    'ноября': '11',
    'декабря': '12'
}

def process_russian_date(raw):
    txt = raw.strip()
    txt = re.sub(r'^[^\d]*', '', txt).strip()
    day = '01'
    m1 = re.search(r'(\d{1,2})\s+([а-яА-Я]+)\s+(\d{4})', txt)
    if m1:
        day = m1.group(1)
        month_word = m1.group(2).lower()
        year = m1.group(3)
    else:
        m2 = re.search(r'([а-яА-Я]+)\s+(\d{4})', txt)
        if m2:
            month_word = m2.group(1).lower()
            year = m2.group(2)
        else:
            return None, "Date parsing failed"
    month = RUSSIAN_MONTHS.get(month_word)
    if not month:
        return None, f"Unknown month: {month_word}"
    day = day.zfill(2)
    return f"{day}/{month}/{year}", "True"

def date_cleaning_all_features(features):
    for feat in features:
        props = feat.get("properties", {})
        # 1) Postcode extraction from address
        address_list = props.get("address", [])
        p_code = ""
        p_status = "Ожидание"
        if isinstance(address_list, list) and address_list:
            raw_html = address_list[0]
            text = re.sub(r'^<[^>]+>', '', raw_html).strip()
            text = re.sub(r'<[^<]+?>', '', text)
            match_start = re.match(r'^(\d{6})', text)
            if match_start:
                p_code = match_start.group(1)
            else:
                matches = re.findall(r'\b\d{6}\b', text)
                if matches:
                    p_code = matches[0]
        if p_code:
            p_status = "True"
        props["postcode"] = p_code
        props["postcodeStatus"] = p_status

        # 2) Date extraction from last sentence of main content
        paragraphs = props.get("main", [])
        d_str = ""
        d_status = "Ожидание"
        if paragraphs:
            combo = ' '.join(paragraphs)
            sentences = [s.strip() for s in combo.split('.') if s.strip()]
            if sentences:
                last_sent = sentences[-1]
                last_sent = re.sub(r'</(b|strong)>\s*', '', last_sent)
                last_sent = re.sub(r'<[^>]+>', '', last_sent).strip()
                last_sent = re.sub(r'\s+года$', '', last_sent).strip()
                last_sent = re.sub(r'^с\s+', '', last_sent, flags=re.IGNORECASE).strip()
                parsed, status_msg = process_russian_date(last_sent)
                if parsed:
                    d_str = parsed
                    d_status = "True"
        props["date"] = d_str
        props["dateStatus"] = d_status
        if "dateRawData" in props:
            del props["dateRawData"]
        feat["properties"] = props

# -----------------------------------------------------------------------------
# UPDATE FEATURES FROM LIVE TELEGRAM SCRAPING
# -----------------------------------------------------------------------------
def get_latest_telegram_address_file():
    files = [f for f in os.listdir(PERSISTENT_DIR) if f.startswith("address_") and f.endswith(".json")]
    valid = []
    for f in files:
        match = re.match(r'address_(\d{2}-\d{2}-\d{4})\.json', f)
        if match:
            try:
                dt = datetime.strptime(match.group(1), '%d-%m-%Y')
                valid.append((f, dt))
            except:
                pass
    if not valid:
        return None, None
    latest_file, latest_date = max(valid, key=lambda x: x[1])
    return os.path.join(PERSISTENT_DIR, latest_file), latest_date

def normalize_url(url):
    url = url.replace("https://storage.googleapis.com/kldscp/", "https://")
    if not url.endswith('/'):
        url = url + '/'
    return url

def scrape_telegram_addresses(min_date=None):
    telegram_url = "https://t.me/s/pzk_memorial"
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/90.0.4430.93 Safari/537.36")
    }
    logger.info("Fetching Telegram page: %s", telegram_url)
    try:
        response = requests.get(telegram_url, headers=headers)
        response.raise_for_status()
        logger.info("Telegram page fetched successfully.")
    except Exception as e:
        logger.error("Error fetching Telegram page: %s", e)
        return None
    soup = BeautifulSoup(response.text, "html.parser")
    messages = soup.find_all("div", class_="tgme_widget_message")
    posts = []
    now = datetime.now(timezone.utc)
    two_weeks_ago = now - timedelta(days=14)
    target_start = "Новые адреса и анонсы вечеров писем политзаключённым"
    for message in messages:
        text_div = message.find("div", class_=lambda x: x and "tgme_widget_message_text" in x)
        if not text_div:
            continue
        text_plain = text_div.get_text(separator="\n").strip()
        lines = [line.strip() for line in text_plain.splitlines() if line.strip()]
        if not lines:
            continue
        if lines[0] == "❗️" and len(lines) > 1:
            first_line = lines[1]
        else:
            first_line = lines[0]
        if not first_line.startswith(target_start):
            continue
        time_tag = message.find("time")
        if time_tag and time_tag.has_attr("datetime"):
            datetime_str = time_tag["datetime"]
            try:
                post_time = parser.isoparse(datetime_str)
            except Exception as e:
                continue
            if post_time < two_weeks_ago:
                continue
            if min_date is not None and post_time <= min_date:
                continue
            posts.append((post_time, text_div))
    if not posts:
        logger.info("No matching Telegram posts found after the cutoff date.")
        return None
    posts.sort(key=lambda x: x[0])
    latest_post_time, latest_text_div = posts[-1]
    address_items = extract_address_items(latest_text_div)
    logger.info("Extracted %d address items from Telegram post dated %s.", len(address_items), latest_post_time.isoformat())
    return latest_post_time, address_items

def extract_address_items(text_div):
    children = [child for child in text_div.children if not (isinstance(child, str) and child.strip() == "")]
    start_index = 0
    for i, child in enumerate(children):
        if getattr(child, "name", None) == "i":
            text = child.get_text(" ", strip=True)
            if re.match(r'^\d{6}', text):
                start_index = i
                break
    relevant = children[start_index:]
    logger.debug("Found %d relevant child elements for addresses", len(relevant))
    items = []
    if len(relevant) < 3:
        logger.debug("Not enough elements to extract addresses.")
        return items
    if relevant[0].name == "i":
        pre_text = relevant[0].get_text(" ", strip=True)
    else:
        pre_text = ""
    index = 1
    while index < len(relevant) - 1:
        if getattr(relevant[index], "name", None) != "a":
            index += 1
            continue
        a_tag = relevant[index]
        link = a_tag.get("href", "").strip()
        link_text = a_tag.get_text(" ", strip=True)
        next_elem = relevant[index + 1]
        if getattr(next_elem, "name", None) != "i":
            index += 1
            continue
        i_tag = next_elem
        inner_html = i_tag.decode_contents()
        parts = re.split(r'<br\s*/?>\s*<br\s*/?>', inner_html, flags=re.IGNORECASE, maxsplit=1)
        post_text = ""
        new_pre = ""
        if parts:
            post_text = BeautifulSoup(parts[0], "html.parser").get_text(" ", strip=True)
            if len(parts) > 1:
                new_pre = BeautifulSoup(parts[1], "html.parser").get_text(" ", strip=True)
        full_address = (pre_text + " " + link_text + " " + post_text).strip()
        m = re.search(r'(\d{6})', pre_text)
        postcode = m.group(1) if m else ""
        items.append({
            "link": link,
            "postcode": postcode,
            "address": full_address
        })
        logger.debug("Extracted item: link=%s, postcode=%s, address=%s", link, postcode, full_address)
        pre_text = new_pre
        index += 2
    return items

def update_new_features_with_telegram(features):
    """
    Scrape Telegram live and update ANY feature (old or new) whose sourceUrl matches a Telegram link.
    Also, save the new Telegram data into a JSON file.
    """
    existing_file, existing_date = get_latest_telegram_address_file()
    if existing_date is not None:
        cutoff = existing_date.replace(tzinfo=timezone.utc)
        logger.info("Existing Telegram address file found: %s with date %s", existing_file, existing_date.strftime("%d-%m-%Y"))
    else:
        cutoff = datetime.min.replace(tzinfo=timezone.utc)
        logger.info("No existing Telegram address file found. Using no cutoff.")
    result = scrape_telegram_addresses(min_date=cutoff)
    if result is None:
        logger.info("No new Telegram posts to update features.")
        return
    post_time, telegram_items = result
    new_file = os.path.join(PERSISTENT_DIR, f"address_{post_time.strftime('%d-%m-%Y')}.json")
    try:
        with open(new_file, "w", encoding="utf-8") as f:
            json.dump(telegram_items, f, ensure_ascii=False, indent=4)
        logger.info("Saved new Telegram addresses to %s", new_file)
    except Exception as e:
        logger.error("Error saving Telegram addresses file: %s", e)
    for item in telegram_items:
        t_link = normalize_url(item.get("link", ""))
        t_postcode = item.get("postcode", "").strip()
        t_address = item.get("address", "").strip()
        for feat in features:
            props = feat.get("properties", {})
            source_url = props.get("sourceUrl", "")
            if normalize_url(source_url) == t_link:
                old_postcode = props.get("postcode", "")
                old_address = props.get("address", [])
                props["postcode"] = t_postcode
                props["address"] = [t_address]
                props["geocodeStatus"] = "Telegram updated"
                feat["properties"] = props
                logger.info("Updated feature ID=%s from Telegram: postcode '%s' -> '%s', address '%s' -> '%s'",
                            props.get("ID", ""), old_postcode, t_postcode, old_address, [t_address])

# -----------------------------------------------------------------------------
# 7) REMOVE UNUSED IMAGES
# -----------------------------------------------------------------------------
def remove_unused_images_in_final_geojson(geojson_data):
    used_fnames = set()
    for feat in geojson_data["features"]:
        props = feat.get("properties", {})
        fname = props.get("imageFilename", "")
        if fname:
            used_fnames.add(fname)
    if os.path.exists(IMAGES_DIR):
        for f in os.listdir(IMAGES_DIR):
            if f not in used_fnames:
                try:
                    os.remove(os.path.join(IMAGES_DIR, f))
                    logger.info(f"Removed unused image => {f}")
                except Exception as e:
                    logger.warning(f"Could not remove image {f}: {e}")

# -----------------------------------------------------------------------------
# Apply manual overrides from overrides.json
# -----------------------------------------------------------------------------
def apply_overrides(geojson_data):
    overrides_file = os.path.join(PERSISTENT_DIR, "overrides.json")
    if os.path.exists(overrides_file):
        try:
            with open(overrides_file, "r", encoding="utf-8") as f:
                overrides = json.load(f)
        except Exception as e:
            log_error(f"Error loading overrides.json: {e}")
            return geojson_data
        for feat in geojson_data.get("features", []):
            props = feat.get("properties", {})
            feat_id = str(props.get("ID", ""))
            if feat_id in overrides:
                override = overrides[feat_id]
                props.update(override)
                logger.info(f"Applied override for ID={feat_id}")
                feat["properties"] = props
        return geojson_data
    else:
        logger.info("No overrides.json file found.")
        return geojson_data

# -----------------------------------------------------------------------------
# CLI commands for manual operations (now exposed via web endpoints)
# -----------------------------------------------------------------------------
def find_id(query):
    latest_geojson_file = get_latest_geojson()
    if not latest_geojson_file:
        return {"error": "No geojson found."}
    data = load_geojson(latest_geojson_file)
    matches = []
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        if (query.lower() in str(props.get("name", "")).lower()) or (query in str(props.get("sourceUrl", ""))):
            matches.append(str(props.get("ID", "")))
    if matches:
        return {"found_ids": matches}
    else:
        return {"message": f"No matching IDs found for query: {query}"}

def geocode_feature(feature, ref_dict):
    try:
        props = feature.get("properties", {})
        page_id = props.get("ID", "")
        rec_postcode = props.get("postcode", "").strip() if "postcode" in props else ""
        chosen_coords = FALLBACK_COORDS[:]
        geo_status = "pending"
        address_field = props.get("address", [])
        candidate_text = " ".join(address_field) if isinstance(address_field, list) else str(address_field)
        if not rec_postcode:
            geo_status = "rf"
            chosen_coords = FALLBACK_COORDS
            logger.info(f"[GEO] ID={page_id} => No postcode => geocodeStatus=rf => coords={chosen_coords}")
        else:
            if rec_postcode not in ref_dict:
                geo_status = "Индекс не найден"
                chosen_coords = FALLBACK_COORDS
                logger.info(f"[GEO] ID={page_id} => Postcode '{rec_postcode}' not in reference => coords={chosen_coords}")
            else:
                candidates = ref_dict[rec_postcode]
                if len(candidates) == 1:
                    cfeat, ctxt = candidates[0]
                    coords = cfeat.get("geometry", {}).get("coordinates", [])
                    if coords and len(coords) == 2:
                        chosen_coords = coords
                        geo_status = "True"
                        logger.info(f"[GEO] ID={page_id} => Single match => coords={chosen_coords} => geocodeStatus=True")
                    else:
                        geo_status = "Индекс не найден"
                        logger.info(f"[GEO] ID={page_id} => Single match but no valid coords => fallback => geocodeStatus={geo_status}")
                else:
                    if INTERACTIVE_MODE:
                        print(f"\n-- Manual Check for ID={page_id} --")
                        print(f"Original Address: {candidate_text}")
                        for i, (candidate_feat, candidate_text_candidate) in enumerate(candidates):
                            this_score = fuzz.token_sort_ratio(candidate_text, candidate_text_candidate)
                            print(f"  [{i}] Score={this_score} => {candidate_text_candidate}")
                        user_input = input("Choose index [or press Enter to skip]: ").strip()
                        if user_input.isdigit():
                            pick_idx = int(user_input)
                            if 0 <= pick_idx < len(candidates):
                                pick_feat = candidates[pick_idx][0]
                                coords = pick_feat.get("geometry", {}).get("coordinates", [])
                                if coords and len(coords) == 2:
                                    chosen_coords = coords
                                    geo_status = "True"
                                    print(f"Chosen index: {pick_idx}, coords={coords}, status=True")
                                    logger.info(f"[GEO] ID={page_id} => user-chosen index={pick_idx} => coords={coords}, geocodeStatus=True")
                                else:
                                    print("No coords in that feature, fallback used.")
                                    logger.info(f"[GEO] ID={page_id} => user-chosen index={pick_idx}, but no coords => fallback used")
                            else:
                                print("Invalid index, fallback used.")
                        else:
                            print("No index chosen, fallback used.")
                        if geo_status != "True":
                            best_str, best_score = fuzzy_match(candidate_text, [x[1] for x in candidates])
                            if best_str and best_score >= FUZZY_THRESHOLD:
                                chosen_ref = next((rfeat for (rfeat, rtxt) in candidates if rtxt == best_str), None)
                                if chosen_ref:
                                    coords = chosen_ref.get("geometry", {}).get("coordinates", [])
                                    if coords and len(coords) == 2:
                                        chosen_coords = coords
                                        geo_status = "True"
                                        logger.info(f"[GEO] ID={page_id} => Fuzzy matched coords={chosen_coords} => geocodeStatus=True")
                                    else:
                                        geo_status = "Индекс не найден"
                                        logger.info(f"[GEO] ID={page_id} => Fuzzy matched but coords missing => fallback => geocodeStatus={geo_status}")
                                else:
                                    geo_status = "Требуется проверка"
                                    logger.info(f"[GEO] ID={page_id} => Could not find feature for bestStr => geocodeStatus={geo_status}")
                            else:
                                geo_status = "Требуется проверка"
                                logger.info(f"[GEO] ID={page_id} => Fuzzy score < {FUZZY_THRESHOLD} => fallback => geocodeStatus={geo_status}")
                    else:
                        best_str, best_score = fuzzy_match(candidate_text, [x[1] for x in candidates])
                        logger.info(f"[GEO] ID={page_id} => Multiple candidates => bestStr='{best_str}', score={best_score}")
                        if best_str and best_score >= FUZZY_THRESHOLD:
                            chosen_ref = next((rfeat for (rfeat, rtxt) in candidates if rtxt == best_str), None)
                            if chosen_ref:
                                coords = chosen_ref.get("geometry", {}).get("coordinates", [])
                                if coords and len(coords) == 2:
                                    chosen_coords = coords
                                    geo_status = "True"
                                    logger.info(f"[GEO] ID={page_id} => Fuzzy matched coords={chosen_coords} => geocodeStatus=True")
                                else:
                                    geo_status = "Индекс не найден"
                                    logger.info(f"[GEO] ID={page_id} => Fuzzy matched but coords missing => fallback => geocodeStatus={geo_status}")
                            else:
                                geo_status = "Требуется проверка"
                                logger.info(f"[GEO] ID={page_id} => Could not find feature for bestStr => geocodeStatus={geo_status}")
                        else:
                            geo_status = "Требуется проверка"
                            logger.info(f"[GEO] ID={page_id} => Fuzzy score < {FUZZY_THRESHOLD} => fallback => geocodeStatus={geo_status}")
        feature["geometry"]["coordinates"] = chosen_coords
        props["geocodeStatus"] = geo_status
        feature["properties"] = props
        return feature
    except Exception as e:
        log_error(f"[GEO] Error geocoding ID={props.get('ID','')}: {e}")
        props["geocodeStatus"] = f"Error: {e}"
        feature["properties"] = props
        return feature

def manual_geocode(feature_id):
    latest_geojson_file = get_latest_geojson()
    if not latest_geojson_file:
        return {"error": "No existing geojson found."}
    data = load_geojson(latest_geojson_file)
    found = False
    updated_status = None
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        if str(props.get("ID", "")) == str(feature_id):
            found = True
            ref_dict = load_reference_geojson(REFERENCE_GEOJSON_FILE)
            updated_feat = geocode_feature(feat, ref_dict)
            feat.update(updated_feat)
            updated_status = {
                "ID": feature_id,
                "geocodeStatus": updated_feat["properties"].get("geocodeStatus"),
                "coordinates": updated_feat["geometry"].get("coordinates")
            }
            break
    if not found:
        return {"error": f"Feature with ID={feature_id} not found."}
    else:
        save_geojson(data, latest_geojson_file)
        return {"message": f"Updated geocoding for ID {feature_id}.", "status": updated_status}

def manual_apply_overrides():
    latest_geojson_file = get_latest_geojson()
    if not latest_geojson_file:
        return {"error": "No geojson found."}
    data = load_geojson(latest_geojson_file)
    updated_data = apply_overrides(data)
    save_geojson(updated_data, latest_geojson_file)
    return {"message": f"Overrides applied. Geojson {latest_geojson_file} updated."}

def generate_overrides(ids_str):
    latest_geojson_file = get_latest_geojson()
    if not latest_geojson_file:
        return {"error": "No geojson found."}
    data = load_geojson(latest_geojson_file)
    id_list = [s.strip() for s in ids_str.split(",") if s.strip()]
    overrides = {}
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        feat_id = str(props.get("ID", ""))
        if feat_id in id_list:
            overrides[feat_id] = props
    if not overrides:
        return {"error": "No matching features found for the given IDs."}
    try:
        overrides_file = os.path.join(PERSISTENT_DIR, "overrides.json")
        with open(overrides_file, "w", encoding="utf-8") as f:
            json.dump(overrides, f, ensure_ascii=False, indent=4)
        return {"message": "overrides.json generated", "ids": list(overrides.keys())}
    except Exception as e:
        return {"error": f"Error writing overrides.json: {e}"}

# -----------------------------------------------------------------------------
# 8) MAIN PIPELINE
# -----------------------------------------------------------------------------
def main():
    # (A) Create merged CSV
    merged_csv = create_merged_csv()
    if not merged_csv:
        log_error("No merged CSV. Exiting.")
        return {"error": "No merged CSV. Exiting."}

    date_str = extract_date_from_filename(merged_csv)
    if not date_str:
        date_str = datetime.now().strftime('%d-%m-%Y')

    # (B) Load existing GeoJSON
    old_geojson_file = get_latest_geojson()  # returns full path or None
    old_data = load_geojson(old_geojson_file) if old_geojson_file else {"type": "FeatureCollection", "features": []}
    old_ids = extract_ids_from_geojson(old_data)

    # (C) Extract IDs from CSV, compare
    csv_ids = extract_ids_from_csv(merged_csv)
    added_ids, removed_ids = diff_ids(csv_ids, old_ids)
    logger.info(f"ADDED IDs: {added_ids}")
    logger.info(f"REMOVED IDs: {removed_ids}")

    new_features = []
    if added_ids:
        # (D) Scrape & process ONLY the added IDs
        new_features = scrape_new_ids(added_ids, merged_csv)
        date_cleaning_all_features(new_features)
        ref_dict = load_reference_geojson(REFERENCE_GEOJSON_FILE)
        geocode_new_features(new_features, ref_dict)
    else:
        logger.info("No new IDs to scrape or geocode.")

    # (E) Remove removed IDs from old GeoJSON
    if removed_ids:
        filtered_features = []
        for feat in old_data.get("features", []):
            props = feat.get("properties", {})
            id_ = str(props.get("ID", ""))
            if id_ not in removed_ids:
                filtered_features.append(feat)
        old_data["features"] = filtered_features

    # (F) Append new features into old_data
    all_features = old_data.get("features", [])
    all_features.extend(new_features)
    old_data["features"] = all_features

    # Update ALL features using LIVE Telegram scraping
    update_new_features_with_telegram(old_data["features"])

    # (F.5) Apply manual overrides if available
    old_data = apply_overrides(old_data)

    # (G) Save final GeoJSON to persistent disk
    final_geojson_name = f"list_{len(csv_ids)}_{date_str}.geojson"
    final_geojson_path = os.path.join(PERSISTENT_DIR, final_geojson_name)
    save_geojson(old_data, final_geojson_path)
    remove_unused_images_in_final_geojson(old_data)
    logger.info("Pipeline complete.")
    logger.info(f"Run Summary: New IDs: {len(added_ids)}, Removed IDs: {len(removed_ids)}, Errors: {ERROR_COUNT}")

    # (H) For newly added features, summarize status fields.
    if new_features:
        new_postcode_status = {}
        new_geocode_status = {}
        new_date_status = {}
        for feat in new_features:
            props = feat.get("properties", {})
            p_status = props.get("postcodeStatus", "N/A")
            g_status = props.get("geocodeStatus", "N/A")
            d_status = props.get("dateStatus", "N/A")
            new_postcode_status[p_status] = new_postcode_status.get(p_status, 0) + 1
            new_geocode_status[g_status] = new_geocode_status.get(g_status, 0) + 1
            new_date_status[d_status] = new_date_status.get(d_status, 0) + 1
        logger.info("New Features Summary: PostcodeStatus: %s", new_postcode_status)
        logger.info("New Features Summary: GeocodeStatus: %s", new_geocode_status)
        logger.info("New Features Summary: DateStatus: %s", new_date_status)

    # Generate manifest.json without markerImages
    manifest = {
        "latestGeojson": final_geojson_name,
        "featuresCount": len(old_data.get("features", [])),
        "latestUpdate": date_str
    }
    manifest_path = os.path.join(PERSISTENT_DIR, "manifest.json")
    try:
        with open(manifest_path, "w", encoding="utf-8") as mf:
            json.dump(manifest, mf, ensure_ascii=False, indent=2)
        logger.info("Manifest saved as manifest.json")
    except Exception as e:
        log_error(f"Error saving manifest.json: {e}")
    return {
        "message": "Pipeline complete.",
        "summary": {
            "new_ids": len(added_ids),
            "removed_ids": len(removed_ids),
            "errors": ERROR_COUNT
        }
    }

# -----------------------------------------------------------------------------
# Flask Web Service Endpoints and Static File Serving
# -----------------------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return "Pipeline Web Service is running"

@app.route("/run", methods=["POST"])
def run_pipeline():
    try:
        result = main()
        return jsonify(result)
    except Exception as e:
        logger.exception("Pipeline run error")
        return jsonify({"error": str(e)}), 500

@app.route("/find", methods=["GET"])
def find_route():
    query = request.args.get("query", "")
    if not query:
        return jsonify({"error": "Missing query parameter"}), 400
    result = find_id(query)
    status_code = 200 if "found_ids" in result or ("message" in result and "No matching" not in result.get("message", "")) else 404
    return jsonify(result), status_code

@app.route("/geocode/<feature_id>", methods=["POST"])
def geocode_route(feature_id):
    result = manual_geocode(feature_id)
    status_code = 200 if "message" in result else 404
    return jsonify(result), status_code

@app.route("/apply_overrides", methods=["POST"])
def apply_overrides_route():
    result = manual_apply_overrides()
    status_code = 200 if "message" in result else 404
    return jsonify(result), status_code

@app.route("/generate_overrides", methods=["POST"])
def generate_overrides_route():
    content = request.get_json()
    if not content or "ids" not in content:
        return jsonify({"error": "JSON payload must contain 'ids' key with comma-separated list of IDs."}), 400
    result = generate_overrides(content["ids"])
    status_code = 200 if "message" in result else 404
    return jsonify(result), status_code

# -----------------------------------------------------------------------------
# Static File Endpoints
# -----------------------------------------------------------------------------
@app.route("/manifest.json")
def serve_manifest():
    return send_from_directory(PERSISTENT_DIR, "manifest.json")

@app.route("/<path:filename>")
def serve_geojson(filename):
    return send_from_directory(PERSISTENT_DIR, filename)

@app.route("/images/<path:filename>")
def serve_images(filename):
    return send_from_directory(IMAGES_DIR, filename)

@app.route("/overrides.json")
def serve_overrides():
    return send_from_directory(PERSISTENT_DIR, "overrides.json")

@app.route("/pipeline.log")
def serve_log():
    return send_from_directory(PERSISTENT_DIR, "pipeline.log")

# -----------------------------------------------------------------------------
# Run the Flask App
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
