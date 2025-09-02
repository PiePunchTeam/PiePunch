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
MAX_THREADS = 2  # Reduced to avoid rate-limiting
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

def get_last_scraped_date():
    try:
        events_df = pd.read_csv('data/event_details.csv')
        if not events_df.empty:
            return events_df['date'].max()
        return '1900-01-01'
    except FileNotFoundError:
        logger.error("event_details.csv not found. Using default date.")
        return '1900-01-01'

last_date = get_last_scraped_date()
logger.info(f"Last scraped date: {last_date}. Scraping new events...")

# Load existing CSVs
try:
    existing_events = pd.read_csv('data/event_details.csv')
    existing_fights = pd.read_csv('data/fight_details.csv')
    existing_fighters = pd.read_csv('data/fighter_details.csv')
except FileNotFoundError:
    existing_events = pd.DataFrame()
    existing_fights = pd.DataFrame()
    existing_fighters = pd.DataFrame()

# Scrape event links
try:
    ufc_link = "http://ufcstats.com/statistics/events/completed?page=all"
    response = session.get(ufc_link)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'lxml')
    event_links_soup = soup.find_all('a', class_='b-link b-link_style_black')
    event_links = [link['href'] for link in event_links_soup]
except Exception as e:
    logger.error(f"Failed to scrape event links: {str(e)}")
    event_links = []

# Filter new events
new_event_links = []
for link in event_links:
    try:
        event_response = session.get(link)
        event_response.raise_for_status()
        event_soup = BeautifulSoup(event_response.text, 'lxml')
        date_loc_list = event_soup.find_all('li', 'b-list__box-list-item')
        date = date_loc_list[0].text.replace("Date:", "").strip()
        if date > last_date:
            new_event_links.append(link)
        time.sleep(2)  # Delay to avoid rate-limiting
    except Exception as e:
        logger.error(f"Failed to check event {link}: {str(e)}")
        continue

logger.info(f"Found {len(new_event_links)} new events.")

def get_event_data(item):
    idx, link = item
    link = link.strip()
    try:
        response = session.get(link, headers=HEADER, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        event_id = link[-16:]
        date_loc_list = soup.find_all('li', 'b-list__box-list-item')
        date = date_loc_list[0].text.replace("Date:", "").strip()
        location = date_loc_list[1].text.replace("Location:", "").strip()
        fight_links = soup.find_all('tr', class_='b-fight-details__table-row b-fight-details__table-row__hover js-fight-details-click')
        with lock:
            for i in fight_links:
                w_l_d = i.find('i', class_="b-flag__text").text
                fight_id = i['data-link'][-16:]
                if fight_id in existing_fights.get('fight_id', []).values:
                    continue  # Skip existing fights
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
            logger.info(f"Scraped event {idx+1}/{len(new_event_links)}: {link}")
        time.sleep(2)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to retrieve event {link}: {str(e)}")

def get_fight_data(item):
    idx, link = item
    link = link.strip()
    try:
        response = session.get(link, headers=HEADER, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        event_name = soup.find('a', class_="b-link").text.strip()
        event_id = soup.find('a', class_="b-link")['href'][-16:]
        fight_id = link[-16:]
        if fight_id in existing_fights.get('fight_id', []).values:
            return  # Skip existing fights
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
        winner_name = None
        winner_id = None
        event_df = pd.DataFrame(winner_names)
        if not event_df[event_df['fight_id'] == fight_id].empty:
            winner_name = event_df[event_df['fight_id'] == fight_id]['winner'].iloc[0]
            winner_id = event_df[event_df['fight_id'] == fight_id]['winner_id'].iloc[0]
        tables = soup.find_all('table', style="width: 745px")
        if len(tables) > 0:
            table1 = tables[0]
            td_1_list = table1.find_all('td', class_='b-fight-details__table-col')
            kd_players = td_1_list[1].text.split()
            r_kd, b_kd = int(kd_players[0]), int(kd_players[1])
            sig_str_players = td_1_list[2].text.split()
            r_sig_str_landed = int(sig_str_players[0])
            r_sig_str_atmpted = int(sig_str_players[2])
            b_sig_str_landed = int(sig_str_players[3])
            b_sig_str_atmpted = int(sig_str_players[5])
            sig_str_acc = td_1_list[3].text.split()
            r_sig_str_acc = int(sig_str_acc[0].replace("%", "")) if sig_str_acc[0] != "---" else None
            b_sig_str_acc = int(sig_str_acc[1].replace("%", "")) if sig_str_acc[1] != "---" else None
            total_str = td_1_list[4].text.split()
            r_total_str_landed = int(total_str[0])
            r_total_str_atmpted = int(total_str[2])
            b_total_str_landed = int(total_str[3])
            b_total_str_atmpted = int(total_str[5])
            r_total_str_acc, b_total_str_acc = None, None
            try:
                r_total_str_acc = int(round(r_total_str_landed / r_total_str_atmpted, 2) * 100)
            except:
                pass
            try:
                b_total_str_acc = int(round(b_total_str_landed / b_total_str_atmpted, 2) * 100)
            except:
                pass
            td_players = td_1_list[5].text.split()
            r_td_landed = int(td_players[0])
            r_td_atmpted = int(td_players[2])
            b_td_landed = int(td_players[3])
            b_td_atmpted = int(td_players[5])
            td_acc = td_1_list[6].text.split()
            r_td_acc = int(td_acc[0].replace("%", "")) if td_acc[0] != "---" else None
            b_td_acc = int(td_acc[1].replace("%", "")) if td_acc[1] != "---" else None
            sub_att = td_1_list[7].text.split()
            r_sub_att, b_sub_att = int(sub_att[0]), int(sub_att[1])
            rev = td_1_list[8].text.split()
            r_rev, b_rev = int(rev[0]), int(rev[1])
            ctrl = td_1_list[9].text.split()
            r_ctrl = ctrl[0].split(":")
            r_ctrl = int(r_ctrl[0]) * 60 + int(r_ctrl[1]) if r_ctrl[0] != '--' else None
            b_ctrl = ctrl[1].split(":")
            b_ctrl = int(b_ctrl[0]) * 60 + int(b_ctrl[1]) if b_ctrl[0] != '--' else None
            table2 = tables[1]
            td_2_list = table2.find_all('td', class_='b-fight-details__table-col')
            head_list = td_2_list[3].text.split()
            r_head_landed = int(head_list[0])
            r_head_atmpted = int(head_list[2])
            b_head_landed = int(head_list[3])
            b_head_atmpted = int(head_list[5])
            r_head_acc, b_head_acc = None, None
            try:
                r_head_acc = int(round(r_head_landed / r_head_atmpted, 2) * 100)
            except:
                pass
            try:
                b_head_acc = int(round(b_head_landed / b_head_atmpted, 2) * 100)
            except:
                pass
            body_list = td_2_list[4].text.split()
            r_body_landed = int(body_list[0])
            r_body_atmpted = int(body_list[2])
            b_body_landed = int(body_list[3])
            b_body_atmpted = int(body_list[5])
            r_body_acc, b_body_acc = None, None
            try:
                r_body_acc = int(round(r_body_landed / r_body_atmpted, 2) * 100)
            except:
                pass
            try:
                b_body_acc = int(round(b_body_landed / b_body_atmpted, 2) * 100)
            except:
                pass
            leg_list = td_2_list[5].text.split()
            r_leg_landed = int(leg_list[0])
            r_leg_atmpted = int(leg_list[2])
            b_leg_landed = int(leg_list[3])
            b_leg_atmpted = int(leg_list[5])
            r_leg_acc, b_leg_acc = None, None
            try:
                r_leg_acc = int(round(r_leg_landed / r_leg_atmpted, 2) * 100)
            except:
                pass
            try:
                b_leg_acc = int(round(b_leg_landed / b_leg_atmpted, 2) * 100)
            except:
                pass
            dist_list = td_2_list[6].text.split()
            r_dist_landed = int(dist_list[0])
            r_dist_atmpted = int(dist_list[2])
            b_dist_landed = int(dist_list[3])
            b_dist_atmpted = int(dist_list[5])
            r_dist_acc, b_dist_acc = None, None
            try:
                r_dist_acc = int(round(r_dist_landed / r_dist_atmpted, 2) * 100)
            except:
                pass
            try:
                b_dist_acc = int(round(b_dist_landed / b_dist_atmpted, 2) * 100)
            except:
                pass
            clinch_list = td_2_list[7].text.split()
            r_clinch_landed = int(clinch_list[0])
            r_clinch_atmpted = int(clinch_list[2])
            b_clinch_landed = int(clinch_list[3])
            b_clinch_atmpted = int(clinch_list[5])
            r_clinch_acc, b_clinch_acc = None, None
            try:
                r_clinch_acc = int(round(r_clinch_landed / r_clinch_atmpted, 2) * 100)
            except:
                pass
            try:
                b_clinch_acc = int(round(b_clinch_landed / b_clinch_atmpted, 2) * 100)
            except:
                pass
            ground_list = td_2_list[8].text.split()
            r_ground_landed = int(ground_list[0])
            r_ground_atmpted = int(ground_list[2])
            b_ground_landed = int(ground_list[3])
            b_ground_atmpted = int(ground_list[5])
            r_ground_acc, b_ground_acc = None, None
            try:
                r_ground_acc = int(round(r_ground_landed / r_ground_atmpted, 2) * 100)
            except:
                pass
            try:
                b_ground_acc = int(round(b_ground_landed / b_ground_atmpted, 2) * 100)
            except:
                pass
            try:
                r_landed_head_and_dist_list = soup.find_all('i', class_="b-fight-details__charts-num b-fight-details__charts-num_style_red b-fight-details__charts-num_pos_left js-red")
                r_landed_head_per = int(r_landed_head_and_dist_list[0].text.strip().replace("%", ""))
                r_landed_dist_per = int(r_landed_head_and_dist_list[1].text.strip().replace("%", ""))
                b_landed_head_and_dist_list = soup.find_all('i', class_="b-fight-details__charts-num b-fight-details__charts-num_style_blue b-fight-details__charts-num_pos_right js-blue")
                b_landed_head_per = int(b_landed_head_and_dist_list[0].text.strip().replace("%", ""))
                b_landed_dist_per = int(b_landed_head_and_dist_list[1].text.strip().replace("%", ""))
            except:
                r_landed_head_per, r_landed_dist_per = None, None
                b_landed_head_per, b_landed_dist_per = None, None
            try:
                r_landed_body_and_clinch_list = soup.find_all('i', class_="b-fight-details__charts-num b-fight-details__charts-num_style_dark-red b-fight-details__charts-num_pos_left js-red")
                r_landed_body_per = int(r_landed_body_and_clinch_list[0].text.strip().replace("%", ""))
                r_landed_clinch_per = int(r_landed_body_and_clinch_list[1].text.strip().replace("%", ""))
                b_landed_body_and_clinch_list = soup.find_all('i', class_="b-fight-details__charts-num b-fight-details__charts-num_style_dark-blue b-fight-details__charts-num_pos_right js-blue")
                b_landed_body_per = int(b_landed_body_and_clinch_list[0].text.strip().replace("%", ""))
                b_landed_clinch_per = int(b_landed_body_and_clinch_list[1].text.strip().replace("%", ""))
            except:
                r_landed_body_per, r_landed_clinch_per = None, None
                b_landed_body_per, b_landed_clinch_per = None, None
            try:
                r_landed_leg_and_ground_list = soup.find_all('i', class_="b-fight-details__charts-num b-fight-details__charts-num_style_light-red b-fight-details__charts-num_pos_left js-red")
                r_landed_leg_per = int(r_landed_leg_and_ground_list[0].text.strip().replace("%", ""))
                r_landed_ground_per = int(r_landed_leg_and_ground_list[1].text.strip().replace("%", ""))
                b_landed_leg_and_ground_list = soup.find_all('i', class_="b-fight-details__charts-num b-fight-details__charts-num_style_light-blue b-fight-details__charts-num_pos_right js-blue")
                b_landed_leg_per = int(b_landed_leg_and_ground_list[0].text.strip().replace("%", ""))
                b_landed_ground_per = int(b_landed_leg_and_ground_list[1].text.strip().replace("%", ""))
            except:
                r_landed_leg_per, r_landed_ground_per = None, None
                b_landed_leg_per, b_landed_ground_per = None, None
            with lock:
                data_dic = {
                    "event_name": event_name,
                    "event_id": event_id,
                    "date": date,
                    "location": location,
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
                    "winner": winner_name,
                    "winner_id": winner_id,
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
                logger.info(f"Scraped fight {idx+1}/{len(new_fight_links_all)}: {link}")
        time.sleep(2)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed fight {idx+1}/{len(new_fight_links_all)}: {link} - {str(e)}")
        return

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
        return

# Run event and fight scraping
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    results = [executor.submit(get_event_data, item) for item in enumerate(new_event_links)]
    for r in results:
        r.result()

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    results = [executor.submit(get_fight_data, item) for item in enumerate(new_fight_links_all)]
    for r in results:
        r.result()

# Scrape fighter data
r_fighter_id = pd.DataFrame(fight_details)['r_id'].unique()
b_fighter_id = pd.DataFrame(fight_details)['b_id'].unique()
all_ids = list(set(list(r_fighter_id) + list(b_fighter_id)))
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    results = [executor.submit(get_fighter_data, item) for item in enumerate(all_ids)]
    for r in results:
        r.result()

# Append new data to existing CSVs
df_winner = pd.DataFrame(data=winner_names)
if not existing_events.empty:
    df_winner = pd.concat([existing_events, df_winner]).drop_duplicates(subset=['event_id', 'fight_id'], keep='last')
df_winner.to_csv("data/event_details.csv", index=False)

df_fight = pd.DataFrame(data=fight_details)
df_fight['date'] = pd.to_datetime(df_fight['date']).dt.strftime("%Y/%m/%d")
if not existing_fights.empty:
    df_fight = pd.concat([existing_fights, df_fight]).drop_duplicates(subset=['fight_id'], keep='last')
df_fight.to_csv("data/fight_details.csv", index=False)

df_fighter = pd.DataFrame(data=fighter_detail_data)
if not existing_fighters.empty:
    df_fighter = pd.concat([existing_fighters, df_fighter]).drop_duplicates(subset=['id'], keep='last')
df_fighter.to_csv("data/fighter_details.csv", index=False)

logger.info(f"Core scraper complete. Updated {len(df_fight)} fights, {len(df_fighter)} fighters.")
print(f"Core scraper complete. Updated {len(df_fight)} fights, {len(df_fighter)} fighters.")