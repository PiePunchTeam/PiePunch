import pandas as pd
import logging

logging.basicConfig(filename='badges_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('badges_assigner')

def assign_badges():
    try:
        fighters_df = pd.read_csv('data/fighters_stats.csv')
        badges = []

        for _, row in fighters_df.iterrows():
            fid = row['id']
            fighter_badges = []

            if row['wins'] > 0:
                if (row['ko_tko_wins'] / row['wins'] > 0.5) and (row['kd'] / row['strikes_attempted'] > 0.01) and (row['splm'] < 3.5):
                    fighter_badges.append('KO Creamer')
                if (row['splm'] > 4.5) and (row['splm_std'] < 1.5) and (row['total_fights'] >= 5):
                    fighter_badges.append('Yes, Chef')
                if (row['td_avg'] > 2.5) and (row['career_td_acc'] > 40) and (row['ctrl_avg'] > 150):
                    fighter_badges.append('Russian Bear')
                if (row['sub_wins'] / row['wins'] > 0.3) and (row['sub_att'] / row['total_fights'] > 0.8):
                    fighter_badges.append('Pie-thon')
                if (row['ground_finish_rate'] > 50) and (row['ground_landed_per_tko'] > 20) and (row['ctrl_avg'] > 90) and (row['total_fights'] >= 5) and (row['ko_tko_wins'] > 0):
                    fighter_badges.append('Doughmaker')
                if ((row['leg_landed_avg'] > 8) or (row['body_landed_avg'] > 10)) and (row['leg_landed_avg'] + row['body_landed_avg'] > 20) and (row['ko_tko_wins'] > 0) and (row['total_fights'] >= 5):
                    fighter_badges.append('Kickin’ Pot Pie')
            if (row['td_def'] > 70) and (row['td_attempts_received_avg'] < 3):
                fighter_badges.append('Greasy')
            if (row['str_def'] > 60) and (row['sapm'] < 3):
                fighter_badges.append('Can’t Touch This')
            if (row['kd_received_avg'] < 0.3) and (row['ko_loss_rate'] < 20):
                fighter_badges.append('Iron Chin')
            if (row['sub_att_received_avg'] < 0.3) and (row['sub_def'] > 90) and (row['never_submitted'] == 1):
                fighter_badges.append('Locksmith')
            if (row['total_fight_time_sec'] / row['total_fights'] > 600) and (row['sig_str_landed_per_sec'] > 0.5):
                fighter_badges.append('The Dogwalker')
            if (row['five_round_fights'] >= 4) and (row['five_round_win_rate'] > 75) and (row['five_round_decision_rate'] > 50) and (row['five_round_wins'] >= 3):
                fighter_badges.append('Champ Rounds')

            badges.append({
                'id': fid,
                'badges': ','.join(fighter_badges) if fighter_badges else None
            })

        df = pd.DataFrame(badges)
        df.to_csv('data/badges.csv', index=False)
        logger.info(f"Generated badges.csv for {len(fighters_df)} fighters.")
        print(f"Generated badges.csv for {len(fighters_df)} fighters.")
    except Exception as e:
        logger.error(f"Failed to assign badges: {str(e)}")
        raise

assign_badges()