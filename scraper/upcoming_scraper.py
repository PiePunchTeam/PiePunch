import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import threading
from concurrent.futures import ThreadPoolExecutor
import logging
import time

# Setup logging
logging.basicConfig(filename='upcoming_scraper_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('upcoming_scraper')

lock = threading.Lock()
upcoming_event_details = []
upcoming_fight_details = []
new_upcoming_fight_links = []
MAX_THREADS = 2
ua = UserAgent()
HEADER = {'User-Agent': ua.chrome}

def create_session():
    session = requests.Session()
    retry_strat = Retry(backoff_factor=15, total=10, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=['GET'])
    adapter = HTTPAdapter(max_retries=retry_strat)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

session = create_session()

def get_existing_ids(file_path, id_field, date_field=None):
    try:
        df = pd.read_csv(file_path)
        ids = set(df[id_field].astype(str).unique())
        max_date = df[date_field].max() if date_field and date_field in df.columns else '1900-01-01'
        return ids, max_date
    except (FileNotFoundError, KeyError):
        logger.error(f"{file_path} not found or missing required columns.")
        return set(), '1900-01-01'

# Load existing CSVs
existing_upcoming_event_ids, last_upcoming_event_date = get_existing_ids('data/upcoming_event_details.csv', 'event_id', 'date')
existing_upcoming_fight_ids = get_existing_ids('data/upcoming_fight_details.csv', 'fight_id')[0]
logger.info(f"Last upcoming event date: {last_upcoming_event_date}. Found {len(existing_upcoming_event_ids)} upcoming events, {len(existing_upcoming_fight_ids)} upcoming fights.")

# Load existing CSVs
try:
    existing_upcoming_events = pd.read_csv('data/upcoming_event_details.csv')
    existing_upcoming_fights = pd.read_csv('data/upcoming_fight_details.csv')
except FileNotFoundError:
    existing_upcoming_events = pd.DataFrame()
    existing_upcoming_fights = pd.DataFrame()

# Scrape upcoming event links
def scrape_event_links():
    try:
        response = session.get("http://ufcstats.com/statistics/events/upcoming")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        event_links_soup = soup.find_all('a', class_='b-link b-link_style_black')
        return [link['href'] for link in event_links_soup]
    except Exception as e:
        logger.error(f"Failed to scrape upcoming event links: {str(e)}")
        return []

upcoming_event_links = scrape_event_links()
logger.info(f"Found {len(upcoming_event_links)} upcoming events.")

# Filter new upcoming events
new_upcoming_event_links = []
for link in upcoming_event_links:
    try:
        event_id = link[-16:]
        if event_id in existing_upcoming_event_ids:
            continue
        event_response = session.get(link)
        event_response.raise_for_status()
        event_soup = BeautifulSoup(event_response.text, 'lxml')
        date_loc_list = event_soup.find_all('li', 'b-list__box-list-item')
        date = date_loc_list[0].text.replace("Date:", "").strip()
        if date > last_upcoming_event_date:
            new_upcoming_event_links.append(link)
        time.sleep(2)
    except Exception as e:
        logger.error(f"Failed to check upcoming event {link}: {str(e)}")
        continue

logger.info(f"Found {len(new_upcoming_event_links)} new upcoming events.")

def get_upcoming_event_data(item):
    idx, link = item
    try:
        response = session.get(link, headers=HEADER, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        event_id = link[-16:]
        if event_id in existing_upcoming_event_ids:
            return
        event_name = soup.find('span', class_='b-content__title-highlight').text.strip()
        date_loc_list = soup.find_all('li', 'b-list__box-list-item')
        date = date_loc_list[0].text.replace("Date:", "").strip()
        location = date_loc_list[1].text.replace("Location:", "").strip()
        fight_links = soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click')
        with lock:
            data_dic = {
                "event_id": event_id,
                "event_name": event_name,
                "date": date,
                "location": location
            }
            upcoming_event_details.append(data_dic)
            for i in fight_links:
                fight_id = i['data-link'][-16:]
                if fight_id in existing_upcoming_fight_ids:
                    continue
                new_upcoming_fight_links.append(i['data-link'])
            logger.info(f"Scraped upcoming event {idx+1}/{len(new_upcoming_event_links)}: {link}")
        time.sleep(2)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to retrieve upcoming event {link}: {str(e)}")

def get_upcoming_fight_data(item):
    idx, link = item
    try:
        response = session.get(link, headers=HEADER, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        event_name = soup.find('a', class_="b-link").text.strip()
        event_id = soup.find('a', class_="b-link")['href'][-16:]
        fight_id = link[-16:]
        if fight_id in existing_upcoming_fight_ids:
            return
        fighter_nams = soup.find_all('a', class_='b-fight-details__person-link')
        r_name = fighter_nams[0].text.strip()
        b_name = fighter_nams[1].text.strip()
        r_id = fighter_nams[0]['href'].strip()[-16:]
        b_id = fighter_nams[1]['href'].strip()[-16:]
        division_info = soup.find('i', class_='b-fight-details__fight-title').text.lower()
        is_title_fight = 0
        if 'title' in division_info:
            is_title_fight = 1
        division_info = division_info.replace('ufc', "").replace("title", "").replace("bout", "").strip()
        with lock:
            data_dic = {
                "event_name": event_name,
                "event_id": event_id,
                "fight_id": fight_id,
                "r_name": r_name,
                "r_id": r_id,
                "b_name": b_name,
                "b_id": b_id,
                "division": division_info,
                "title_fight": is_title_fight
            }
            upcoming_fight_details.append(data_dic)
            logger.info(f"Scraped upcoming fight {idx+1}/{len(new_upcoming_fight_links)}: {link}")
        time.sleep(2)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed upcoming fight {idx+1}/{len(new_upcoming_fight_links)}: {link} - {str(e)}")

# Run upcoming event and fight scraping
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    results = [executor.submit(get_upcoming_event_data, item) for item in enumerate(new_upcoming_event_links)]
    for r in results:
        r.result()

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    results = [executor.submit(get_upcoming_fight_data, item) for item in enumerate(new_upcoming_fight_links)]
    for r in results:
        r.result()

# Append new data to CSVs
if upcoming_event_details:
    df_upcoming_event = pd.DataFrame(data=upcoming_event_details)
    if not existing_upcoming_events.empty:
        df_upcoming_event = pd.concat([existing_upcoming_events, df_upcoming_event]).drop_duplicates(subset=['event_id'], keep='last')
    df_upcoming_event.to_csv("data/upcoming_event_details.csv", index=False)
else:
    logger.info("No new upcoming events to save.")

if upcoming_fight_details:
    df_upcoming_fight = pd.DataFrame(data=upcoming_fight_details)
    if not existing_upcoming_fights.empty:
        df_upcoming_fight = pd.concat([existing_upcoming_fights, df_upcoming_fight]).drop_duplicates(subset=['fight_id'], keep='last')
    df_upcoming_fight.to_csv("data/upcoming_fight_details.csv", index=False)
else:
    logger.info("No new upcoming fights to save.")

logger.info(f"Upcoming scraper complete. Updated {len(upcoming_event_details)} upcoming events, {len(upcoming_fight_details)} upcoming fights.")
print(f"Upcoming scraper complete. Updated {len(upcoming_event_details)} upcoming events, {len(upcoming_fight_details)} upcoming fights.")