import pandas as pd
import logging

logging.basicConfig(filename='defensive_stats_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('defensive_stats')

def calculate_defensive_stats():
    try:
        fight_df = pd.read_csv('data/fight_details.csv')
        event_df = pd.read_csv('data/event_details.csv')
        fight_df = fight_df.merge(event_df[['fight_id', 'winner_id']], on='fight_id', how='left')
        fighter_ids = pd.concat([fight_df['r_id'], fight_df['b_id']]).unique()
        defensive_stats = []

        for fid in fighter_ids:
            r_fights = fight_df[fight_df['r_id'] == fid]
            b_fights = fight_df[fight_df['b_id'] == fid]
            total_fight_time_sec = 0
            kd_received = 0
            td_attempts_received = 0
            sub_att_received = 0
            sub_def = 0

            for _, row in r_fights.iterrows():
                total_fight_time_sec += row['match_time_sec']
                kd_received += row['b_kd'] if pd.notnull(row['b_kd']) else 0
                td_attempts = row['b_td_atmpted'] if pd.notnull(row['b_td_atmpted']) else 0
                if td_attempts > 10:
                    logger.warning(f"Suspicious b_td_atmpted: {td_attempts} for fight_id {row['fight_id']}")
                td_attempts_received += td_attempts
                sub_att_received += row['b_sub_att'] if pd.notnull(row['b_sub_att']) else 0
                if row['method'].lower() != 'submission' or row['winner_id'] == fid:
                    sub_def += row['b_sub_att'] if pd.notnull(row['b_sub_att']) else 0

            for _, row in b_fights.iterrows():
                total_fight_time_sec += row['match_time_sec']
                kd_received += row['r_kd'] if pd.notnull(row['r_kd']) else 0
                td_attempts = row['r_td_atmpted'] if pd.notnull(row['r_td_atmpted']) else 0
                if td_attempts > 10:
                    logger.warning(f"Suspicious r_td_atmpted: {td_attempts} for fight_id {row['fight_id']}")
                td_attempts_received += td_attempts
                sub_att_received += row['r_sub_att'] if pd.notnull(row['r_sub_att']) else 0
                if row['method'].lower() != 'submission' or row['winner_id'] == fid:
                    sub_def += row['r_sub_att'] if pd.notnull(row['r_sub_att']) else 0

            if total_fight_time_sec > 0:
                minutes = total_fight_time_sec / 60
                kd_received_avg = (kd_received / minutes) * 15
                td_attempts_received_avg = (td_attempts_received / minutes) * 15
                sub_att_received_avg = (sub_att_received / minutes) * 15
                sub_def = (sub_def / (sub_att_received or 1)) * 100
            else:
                kd_received_avg = td_attempts_received_avg = sub_att_received_avg = sub_def = 0

            defensive_stats.append({
                'id': fid,
                'kd_received_avg': round(kd_received_avg, 2),
                'td_attempts_received_avg': round(td_attempts_received_avg, 2),
                'sub_att_received_avg': round(sub_att_received_avg, 2),
                'sub_def': round(sub_def, 2)
            })

        df = pd.DataFrame(defensive_stats)
        df.to_csv('data/defensive_stats.csv', index=False)
        logger.info(f"Generated defensive_stats.csv for {len(fighter_ids)} fighters.")
        print(f"Generated defensive_stats.csv for {len(fighter_ids)} fighters.")
    except Exception as e:
        logger.error(f"Failed to calculate defensive stats: {str(e)}")
        raise

calculate_defensive_stats()