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
logging.basicConfig(filename='core_scraper_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('core_scraper')

lock = threading.Lock()
fight_details = []
new_fight_links_all = []
winner_names = []
fighter_detail_data = []
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
        return ids, max_date, len(df)
    except (FileNotFoundError, KeyError):
        logger.error(f"{file_path} not found or missing required columns.")
        return set(), '1900-01-01', 0

# Load existing CSVs
existing_event_ids, last_event_date, _ = get_existing_ids('data/event_details.csv', 'event_id', 'date')
existing_fight_ids = get_existing_ids('data/fight_details.csv', 'fight_id')[0]
existing_fighter_ids, _, fighter_count = get_existing_ids('data/fighter_details.csv', 'id')
logger.info(f"Last completed event date: {last_event_date}. Found {len(existing_event_ids)} events, {len(existing_fight_ids)} fights, {len(existing_fighter_ids)} fighters ({fighter_count} in fighter_details.csv).")

# Load existing CSVs
try:
    existing_events = pd.read_csv('data/event_details.csv')
    existing_fights = pd.read_csv('data/fight_details.csv')
    existing_fighters = pd.read_csv('data/fighter_details.csv')
except FileNotFoundError:
    existing_events = pd.DataFrame()
    existing_fights = pd.DataFrame()
    existing_fighters = pd.DataFrame()

# Scrape all fighter links
def scrape_fighter_links():
    try:
        response = session.get("http://ufcstats.com/statistics/fighters?sort=rank-desc&page=all")
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        fighter_links_soup = soup.find_all('a', class_='b-link b-link_style_black')
        return [link['href'] for link in fighter_links_soup if '/fighter-details/' in link['href']]
    except Exception as e:
        logger.error(f"Failed to scrape fighter links: {str(e)}")
        return []

# Scrape event links
def scrape_event_links(url):
    try:
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        event_links_soup = soup.find_all('a', class_='b-link b-link_style_black')
        return [link['href'] for link in event_links_soup]
    except Exception as e:
        logger.error(f"Failed to scrape event links from {url}: {str(e)}")
        return []

completed_event_links = scrape_event_links("http://ufcstats.com/statistics/events/completed?page=all")
logger.info(f"Found {len(completed_event_links)} completed events.")

# Filter new completed events
new_completed_event_links = []
for link in completed_event_links:
    try:
        event_id = link[-16:]
        if event_id in existing_event_ids:
            continue
        event_response = session.get(link)
        event_response.raise_for_status()
        event_soup = BeautifulSoup(event_response.text, 'lxml')
        date_loc_list = event_soup.find_all('li', 'b-list__box-list-item')
        date = date_loc_list[0].text.replace("Date:", "").strip()
        if date > last_event_date:
            new_completed_event_links.append(link)
        time.sleep(2)
    except Exception as e:
        logger.error(f"Failed to check completed event {link}: {str(e)}")
        continue

logger.info(f"Found {len(new_completed_event_links)} new completed events.")

def get_completed_event_data(item):
    idx, link = item
    try:
        response = session.get(link, headers=HEADER, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        event_id = link[-16:]
        if event_id in existing_event_ids:
            return
        date_loc_list = soup.find_all('li', 'b-list__box-list-item')
        date = date_loc_list[0].text.replace("Date:", "").strip()
        location = date_loc_list[1].text.replace("Location:", "").strip()
        fight_links = soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click')
        with lock:
            for i in fight_links:
                w_l_d = i.find('i', class_="b-flag__text").text
                fight_id = i['data-link'][-16:]
                if fight_id in existing_fight_ids:
                    continue
                winner_name = None
                winner_id = None
                if w_l_d == "win":
                    players = i.find('td', class_="b-fight-details__table-col l-page_align_left")
                    players = players.find_all('a', class_="b-link b-link_style_black")
                    winner_name = players[0].text.strip()
                    winner_id = players[0]['href'][-16:]
                    data_dic = {
                        "event_id": event_id,
                        "fight_id": fight_id,
                        "date": date,
                        "location": location,
                        "winner": winner_name,
                        "winner_id": winner_id
                    }
                    if fight_id not in [d['fight_id'] for d in winner_names]:
                        new_fight_links_all.append(i['data-link'])
                        winner_names.append(data_dic)
            logger.info(f"Scraped completed event {idx+1}/{len(new_completed_event_links)}: {link}")
        time.sleep(2)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to retrieve completed event {link}: {str(e)}")

def get_completed_fight_data(item):
    idx, link = item
    try:
        response = session.get(link, headers=HEADER, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        event_name = soup.find('a', class_="b-link").text.strip()
        event_id = soup.find('a', class_="b-link")['href'][-16:]
        fight_id = link[-16:]
        if fight_id in existing_fight_ids:
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
        method = soup.find('i', style='font-style: normal').text.strip()
        p_tag_with_fight_detail = soup.find('p', class_="b-fight-details__text")
        fight_details_list = p_tag_with_fight_detail.find_all('i', class_='b-fight-details__text-item')
        finish_round = int(fight_details_list[0].text.lower().replace("round:", "").strip())
        match_timestamp = fight_details_list[1].text.lower().replace("time:", "").strip()
        match_timestamp_splited = match_timestamp.split(":")
        match_time_sec = int(match_timestamp_splited[0]) * 60 + int(match_timestamp_splited[-1])
        total_rounds = fight_details_list[2].text.lower().replace("time format:", "").strip()
        if total_rounds.lower() == "no time limit":
            total_rounds = None
        else:
            total_rounds = int(total_rounds[0])
        referee = fight_details_list[3].text.replace("Referee:", "").strip()
        fight_stat = soup.find_all('p', class_="b-fight-details__table-text")
        r_kd = float(fight_stat[2].text.strip())
        b_kd = float(fight_stat[3].text.strip())
        r_sig_str_landed = float(fight_stat[4].text.strip().split(" of ")[0])
        r_sig_str_atmpted = float(fight_stat[4].text.strip().split(" of ")[1])
        r_sig_str_acc = float(fight_stat[6].text.strip().replace("%", ""))
        b_sig_str_landed = float(fight_stat[5].text.strip().split(" of ")[0])
        b_sig_str_atmpted = float(fight_stat[5].text.strip().split(" of ")[1])
        b_sig_str_acc = float(fight_stat[7].text.strip().replace("%", ""))
        r_total_str_landed = float(fight_stat[8].text.strip().split(" of ")[0])
        r_total_str_atmpted = float(fight_stat[8].text.strip().split(" of ")[1])
        r_total_str_acc = float(fight_stat[10].text.strip().replace("%", ""))
        b_total_str_landed = float(fight_stat[9].text.strip().split(" of ")[0])
        b_total_str_atmpted = float(fight_stat[9].text.strip().split(" of ")[1])
        b_total_str_acc = float(fight_stat[11].text.strip().replace("%", ""))
        r_td_landed = float(fight_stat[12].text.strip().split(" of ")[0])
        r_td_atmpted = float(fight_stat[12].text.strip().split(" of ")[1])
        r_td_acc = float(fight_stat[14].text.strip().replace("%", ""))
        b_td_landed = float(fight_stat[13].text.strip().split(" of ")[0])
        b_td_atmpted = float(fight_stat[13].text.strip().split(" of ")[1])
        b_td_acc = float(fight_stat[15].text.strip().replace("%", ""))
        r_sub_att = float(fight_stat[16].text.strip())
        b_sub_att = float(fight_stat[17].text.strip())
        r_ctrl = fight_stat[20].text.strip()
        r_ctrl = sum([int(x) * 60 + int(y) for x, y in [r_ctrl.split(":")]]) if ":" in r_ctrl else 0
        b_ctrl = fight_stat[21].text.strip()
        b_ctrl = sum([int(x) * 60 + int(y) for x, y in [b_ctrl.split(":")]]) if ":" in b_ctrl else 0
        r_head_landed = float(fight_stat[22].text.strip().split(" of ")[0])
        r_head_atmpted = float(fight_stat[22].text.strip().split(" of ")[1])
        r_head_acc = float(fight_stat[24].text.strip().replace("%", ""))
        b_head_landed = float(fight_stat[23].text.strip().split(" of ")[0])
        b_head_atmpted = float(fight_stat[23].text.strip().split(" of ")[1])
        b_head_acc = float(fight_stat[25].text.strip().replace("%", ""))
        r_body_landed = float(fight_stat[26].text.strip().split(" of ")[0])
        r_body_atmpted = float(fight_stat[26].text.strip().split(" of ")[1])
        r_body_acc = float(fight_stat[28].text.strip().replace("%", ""))
        b_body_landed = float(fight_stat[27].text.strip().split(" of ")[0])
        b_body_atmpted = float(fight_stat[27].text.strip().split(" of ")[1])
        b_body_acc = float(fight_stat[29].text.strip().replace("%", ""))
        r_leg_landed = float(fight_stat[30].text.strip().split(" of ")[0])
        r_leg_atmpted = float(fight_stat[30].text.strip().split(" of ")[1])
        r_leg_acc = float(fight_stat[32].text.strip().replace("%", ""))
        b_leg_landed = float(fight_stat[31].text.strip().split(" of ")[0])
        b_leg_atmpted = float(fight_stat[31].text.strip().split(" of ")[1])
        b_leg_acc = float(fight_stat[33].text.strip().replace("%", ""))
        r_dist_landed = float(fight_stat[34].text.strip().split(" of ")[0])
        r_dist_atmpted = float(fight_stat[34].text.strip().split(" of ")[1])
        r_dist_acc = float(fight_stat[36].text.strip().replace("%", ""))
        b_dist_landed = float(fight_stat[35].text.strip().split(" of ")[0])
        b_dist_atmpted = float(fight_stat[35].text.strip().split(" of ")[1])
        b_dist_acc = float(fight_stat[37].text.strip().replace("%", ""))
        r_clinch_landed = float(fight_stat[38].text.strip().split(" of ")[0])
        r_clinch_atmpted = float(fight_stat[38].text.strip().split(" of ")[1])
        r_clinch_acc = float(fight_stat[40].text.strip().replace("%", ""))
        b_clinch_landed = float(fight_stat[39].text.strip().split(" of ")[0])
        b_clinch_atmpted = float(fight_stat[39].text.strip().split(" of ")[1])
        b_clinch_acc = float(fight_stat[41].text.strip().replace("%", ""))
        r_ground_landed = float(fight_stat[42].text.strip().split(" of ")[0])
        r_ground_atmpted = float(fight_stat[42].text.strip().split(" of ")[1])
        r_ground_acc = float(fight_stat[44].text.strip().replace("%", ""))
        b_ground_landed = float(fight_stat[43].text.strip().split(" of ")[0])
        b_ground_atmpted = float(fight_stat[43].text.strip().split(" of ")[1])
        b_ground_acc = float(fight_stat[45].text.strip().replace("%", ""))
        r_landed_head_per = float(fight_stat[46].text.strip().replace("%", ""))
        r_landed_body_per = float(fight_stat[48].text.strip().replace("%", ""))
        r_landed_leg_per = float(fight_stat[50].text.strip().replace("%", ""))
        r_landed_dist_per = float(fight_stat[52].text.strip().replace("%", ""))
        r_landed_clinch_per = float(fight_stat[54].text.strip().replace("%", ""))
        r_landed_ground_per = float(fight_stat[56].text.strip().replace("%", ""))
        b_landed_head_per = float(fight_stat[47].text.strip().replace("%", ""))
        b_landed_body_per = float(fight_stat[49].text.strip().replace("%", ""))
        b_landed_leg_per = float(fight_stat[51].text.strip().replace("%", ""))
        b_landed_dist_per = float(fight_stat[53].text.strip().replace("%", ""))
        b_landed_clinch_per = float(fight_stat[55].text.strip().replace("%", ""))
        b_landed_ground_per = float(fight_stat[57].text.strip().replace("%", ""))
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
                "title_fight": is_title_fight,
                "method": method,
                "finish_round": finish_round,
                "match_time_sec": match_time_sec,
                "total_rounds": total_rounds,
                "referee": referee,
                "r_kd": r_kd,
                "r_sig_str_landed": r_sig_str_landed,
                "r_sig_str_atmpted": r_sig_str_atmpted,
                "r_sig_str_acc": r_sig_str_acc,
                "r_total_str_landed": r_total_str_landed,
                "r_total_str_atmpted": r_total_str_atmpted,
                "r_total_str_acc": r_total_str_acc,
                "r_td_landed": r_td_landed,
                "r_td_atmpted": r_td_atmpted,
                "r_td_acc": r_td_acc,
                "r_sub_att": r_sub_att,
                "r_ctrl": r_ctrl,
                "r_head_landed": r_head_landed,
                "r_head_atmpted": r_head_atmpted,
                "r_head_acc": r_head_acc,
                "r_body_landed": r_body_landed,
                "r_body_atmpted": r_body_atmpted,
                "r_body_acc": r_body_acc,
                "r_leg_landed": r_leg_landed,
                "r_leg_atmpted": r_leg_atmpted,
                "r_leg_acc": r_leg_acc,
                "r_dist_landed": r_dist_landed,
                "r_dist_atmpted": r_dist_atmpted,
                "r_dist_acc": r_dist_acc,
                "r_clinch_landed": r_clinch_landed,
                "r_clinch_atmpted": r_clinch_atmpted,
                "r_clinch_acc": r_clinch_acc,
                "r_ground_landed": r_ground_landed,
                "r_ground_atmpted": r_ground_atmpted,
                "r_ground_acc": r_ground_acc,
                "r_landed_head_per": r_landed_head_per,
                "r_landed_body_per": r_landed_body_per,
                "r_landed_leg_per": r_landed_leg_per,
                "r_landed_dist_per": r_landed_dist_per,
                "r_landed_clinch_per": r_landed_clinch_per,
                "r_landed_ground_per": r_landed_ground_per,
                "b_kd": b_kd,
                "b_sig_str_landed": b_sig_str_landed,
                "b_sig_str_atmpted": b_sig_str_atmpted,
                "b_sig_str_acc": b_sig_str_acc,
                "b_total_str_landed": b_total_str_landed,
                "b_total_str_atmpted": b_total_str_atmpted,
                "b_total_str_acc": b_total_str_acc,
                "b_td_landed": b_td_landed,
                "b_td_atmpted": b_td_atmpted,
                "b_td_acc": b_td_acc,
                "b_sub_att": b_sub_att,
                "b_ctrl": b_ctrl,
                "b_head_landed": b_head_landed,
                "b_head_atmpted": b_head_atmpted,
                "b_head_acc": b_head_acc,
                "b_body_landed": b_body_landed,
                "b_body_atmpted": b_body_atmpted,
                "b_body_acc": b_body_acc,
                "b_leg_landed": b_leg_landed,
                "b_leg_atmpted": b_leg_atmpted,
                "b_leg_acc": b_leg_acc,
                "b_dist_landed": b_dist_landed,
                "b_dist_atmpted": b_dist_atmpted,
                "b_dist_acc": b_dist_acc,
                "b_clinch_landed": b_clinch_landed,
                "b_clinch_atmpted": b_clinch_atmpted,
                "b_clinch_acc": b_clinch_acc,
                "b_ground_landed": b_ground_landed,
                "b_ground_atmpted": b_ground_atmpted,
                "b_ground_acc": b_ground_acc,
                "b_landed_head_per": b_landed_head_per,
                "b_landed_body_per": b_landed_body_per,
                "b_landed_leg_per": b_landed_leg_per,
                "b_landed_dist_per": b_landed_dist_per,
                "b_landed_clinch_per": b_landed_clinch_per,
                "b_landed_ground_per": b_landed_ground_per
            }
            fight_details.append(data_dic)
            logger.info(f"Scraped completed fight {idx+1}/{len(new_fight_links_all)}: {link}")
        time.sleep(2)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed completed fight {idx+1}/{len(new_fight_links_all)}: {link} - {str(e)}")

def get_fighter_data(item):
    idx, id = item
    try:
        response = session.get(f"http://ufcstats.com/fighter-details/{id}", headers=HEADER, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        fighter_id = id
        fighter_name = soup.find('span', class_='b-content__title-highlight').text.strip()
        fighter_nick_name = soup.find('p', class_="b-content__Nickname").text.strip()
        fighter_record = soup.find('span', class_="b-content__title-record").text.replace("Record:", "").strip().split('-')
        fighter_wins = int(fighter_record[0].split()[0])
        fighter_losses = int(fighter_record[1].split()[0])
        fighter_draws = int(fighter_record[2].split()[0])
        detail_list = soup.find_all('li', class_="b-list__box-list-item b-list__box-list-item_type_block")
        try:
            height = detail_list[0].text.replace("Height:", "").strip().replace("'", "").replace('"', '').split()
            height = round((int(height[0]) * 12 + int(height[1])) * 2.54, 2)
        except:
            height = None
        try:
            weight = detail_list[1].text.replace("Weight:", "").strip().replace(" lbs", "")
            weight = round(float(weight) * 0.45359237, 2)
        except:
            weight = None
        try:
            reach = detail_list[2].text.replace("Reach:", "").strip().replace('"', "")
            reach = round(int(reach) * 2.54, 2)
        except:
            reach = None
        try:
            stance = detail_list[3].text.replace("STANCE:", "").strip()
            stance = stance if stance != "" else None
        except:
            stance = None
        try:
            dob = detail_list[4].text.replace("DOB:", "").strip()
            dob = dob if dob != "--" else None
        except:
            dob = None
        splm = float(detail_list[5].text.replace("SLpM:", "").strip())
        str_acc = int(detail_list[6].text.replace("Str. Acc.:", "").strip().replace("%", ""))
        sapm = float(detail_list[7].text.replace("SApM:", "").strip())
        str_def = int(detail_list[8].text.replace("Str. Def:", "").strip().replace("%", ""))
        td_avg = float(detail_list[10].text.replace("TD Avg.:", "").strip())
        td_acc = int(detail_list[11].text.replace("TD Acc.:", "").strip().replace("%", ""))
        td_def = int(detail_list[12].text.replace("TD Def.:", "").strip().replace("%", ""))
        sub_avg = float(detail_list[13].text.replace("Sub. Avg.:", "").strip())
        with lock:
            data_dic = {
                "id": fighter_id,
                "name": fighter_name,
                "nick_name": fighter_nick_name,
                "wins": fighter_wins,
                "losses": fighter_losses,
                "draws": fighter_draws,
                "height": height,
                "weight": weight,
                "reach": reach,
                "stance": stance,
                "dob": dob,
                "splm": splm,
                "str_acc": str_acc,
                "sapm": sapm,
                "str_def": str_def,
                "td_avg": td_avg,
                "td_acc": td_acc,
                "td_def": td_def,
                "sub_avg": sub_avg
            }
            fighter_detail_data.append(data_dic)
            logger.info(f"Scraped fighter {idx+1}/{len(all_ids)}: {id}")
        time.sleep(2)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed fighter {idx+1}/{len(all_ids)}: {id} - {str(e)}")

# Scrape all fighters if fighter_details.csv is incomplete
if fighter_count < 2000:  # Threshold to trigger full fighter scrape
    logger.info("fighter_details.csv has fewer than 2000 fighters, scraping all fighters")
    fighter_links = scrape_fighter_links()
    all_ids = [link[-16:] for link in fighter_links]
else:
    fight_details_df = pd.DataFrame(fight_details)
    r_fighter_id = fight_details_df['r_id'].unique() if 'r_id' in fight_details_df else []
    b_fighter_id = fight_details_df['b_id'].unique() if 'b_id' in fight_details_df else []
    all_ids = list(set(list(r_fighter_id) + list(b_fighter_id)))

# Run event and fight scraping
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    results = [executor.submit(get_completed_event_data, item) for item in enumerate(new_completed_event_links)]
    for r in results:
        r.result()

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    results = [executor.submit(get_completed_fight_data, item) for item in enumerate(new_fight_links_all)]
    for r in results:
        r.result()

# Scrape fighter data
if all_ids:
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results = [executor.submit(get_fighter_data, item) for item in enumerate(all_ids)]
        for r in results:
            r.result()
else:
    logger.info("No new fighters to scrape.")

# Append new data to CSVs
if winner_names:
    df_winner = pd.DataFrame(data=winner_names)
    if not existing_events.empty:
        df_winner = pd.concat([existing_events, df_winner]).drop_duplicates(subset=['event_id', 'fight_id'], keep='last')
    df_winner.to_csv("data/event_details.csv", index=False)
else:
    logger.info("No new completed events to save.")

if fight_details:
    df_fight = pd.DataFrame(data=fight_details)
    df_fight['date'] = pd.to_datetime(df_fight['date']).dt.strftime("%Y/%m/%d")
    if not existing_fights.empty:
        df_fight = pd.concat([existing_fights, df_fight]).drop_duplicates(subset=['fight_id'], keep='last')
    df_fight.to_csv("data/fight_details.csv", index=False)
else:
    logger.info("No new completed fights to save.")

if fighter_detail_data:
    df_fighter = pd.DataFrame(data=fighter_detail_data)
    if not existing_fighters.empty:
        df_fighter = pd.concat([existing_fighters, df_fighter]).drop_duplicates(subset=['id'], keep='last')
    df_fighter.to_csv("data/fighter_details.csv", index=False)
else:
    logger.info("No new fighters to save.")

logger.info(f"Core scraper complete. Updated {len(fight_details)} completed fights, {len(fighter_detail_data)} fighters.")
print(f"Core scraper complete. Updated {len(fight_details)} completed fights, {len(fighter_detail_data)} fighters.")