import pandas as pd
import logging
import os

logging.basicConfig(filename='badges_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('badges_assigner')

def assign_badges():
    try:
        file_path = 'data/fighters_stats.csv'
        abs_file_path = os.path.abspath(file_path)
        if not os.path.exists(abs_file_path):
            logger.error(f"File not found: {abs_file_path}")
            raise FileNotFoundError(f"File not found: {abs_file_path}")
        
        logger.info(f"Loading file: {abs_file_path}")
        fighters_df = pd.read_csv(file_path)
        logger.info(f"Loaded columns: {list(fighters_df.columns)}")
        
        badges = []
        yes_chef_count = 0

        for _, row in fighters_df.iterrows():
            fid = row['id']
            fighter_badges = []

            wins = row['wins'] if pd.notnull(row['wins']) else 0
            ko_tko_wins = row['ko_tko_wins'] if pd.notnull(row['ko_tko_wins']) else 0
            sub_wins = row['sub_wins'] if pd.notnull(row['sub_wins']) else 0
            total_fights = row['total_fights'] if pd.notnull(row['total_fights']) else 0
            strikes_attempted = row['strikes_attempted'] if pd.notnull(row['strikes_attempted']) else 0
            kd = row['kd'] if pd.notnull(row['kd']) else 0
            splm = row['splm'] if pd.notnull(row['splm']) else 0
            splm_std = row['splm_std'] if pd.notnull(row['splm_std']) else 0
            td_avg = row['td_avg'] if pd.notnull(row['td_avg']) else 0
            career_td_acc = row['career_td_acc'] if pd.notnull(row['career_td_acc']) else 0
            ctrl_avg = row['ctrl_avg'] if pd.notnull(row['ctrl_avg']) else 0
            sub_att = row['sub_att'] if pd.notnull(row['sub_att']) else 0
            ground_finish_rate = row['ground_finish_rate'] if pd.notnull(row['ground_finish_rate']) else 0
            ground_landed_per_tko = row['ground_landed_per_tko'] if pd.notnull(row['ground_landed_per_tko']) else 0
            leg_landed_avg = row['leg_landed_avg'] if pd.notnull(row['leg_landed_avg']) else 0
            body_landed_avg = row['body_landed_avg'] if pd.notnull(row['body_landed_avg']) else 0
            td_def = row['td_def'] if pd.notnull(row['td_def']) else 0
            td_attempts_received_avg = row['td_attempts_received_avg'] if pd.notnull(row['td_attempts_received_avg']) else 0
            str_def = row['str_def'] if pd.notnull(row['str_def']) else 0
            sapm = row['sapm'] if pd.notnull(row['sapm']) else 0
            kd_received_avg = row['kd_received_avg'] if pd.notnull(row['kd_received_avg']) else 0
            ko_loss_rate = row['ko_loss_rate'] if pd.notnull(row['ko_loss_rate']) else 0
            sub_att_received_avg = row['sub_att_received_avg'] if pd.notnull(row['sub_att_received_avg']) else 0
            sub_def = row['sub_def'] if pd.notnull(row['sub_def']) else 0
            never_submitted = row['never_submitted'] if pd.notnull(row['never_submitted']) else 0
            total_fight_time_sec = row['total_fight_time_sec'] if pd.notnull(row['total_fight_time_sec']) else 0
            sig_str_landed_per_sec = row['sig_str_landed_per_sec'] if pd.notnull(row['sig_str_landed_per_sec']) else 0
            five_round_fights = row['five_round_fights'] if pd.notnull(row['five_round_fights']) else 0
            five_round_wins = row['five_round_wins'] if pd.notnull(row['five_round_wins']) else 0
            five_round_decision_rate = row['five_round_decision_rate'] if pd.notnull(row['five_round_decision_rate']) else 0
            five_round_win_rate = row['five_round_win_rate'] if pd.notnull(row['five_round_win_rate']) else 0

            if wins > 0:
                ko_tko_ratio = ko_tko_wins / wins
                sub_wins_ratio = sub_wins / wins
                if (ko_tko_ratio > 0.25) and (strikes_attempted > 0 and kd / strikes_attempted > 0.002) and (splm < 6.5):
                    fighter_badges.append('KO Creamer')
                if (splm > 3.5) and (splm_std < 50.0) and (total_fights >= 5):
                    fighter_badges.append('Yes, Chef')
                    yes_chef_count += 1
                    logger.info(f"Yes, Chef awarded to fighter {fid}: splm={splm}, splm_std={splm_std}, total_fights={total_fights}")
                if (td_avg > 2.8) and (career_td_acc > 42) and (ctrl_avg > 200):
                    fighter_badges.append('Russian Bear')
                if (sub_wins_ratio > 0.15) and (sub_att / total_fights > 0.5):
                    fighter_badges.append('Pie-thon')
                if (ground_finish_rate > 50) and (ground_landed_per_tko > 12) and (ctrl_avg > 100) and (total_fights >= 5) and (ko_tko_wins > 0):
                    fighter_badges.append('Doughmaker')
                if ((leg_landed_avg > 15) or (body_landed_avg > 18)) and (leg_landed_avg + body_landed_avg > 35) and (ko_tko_wins > 1) and (total_fights >= 7):
                    fighter_badges.append('Kickin’ Pot Pie')
            if (td_def > 78) and (td_attempts_received_avg < 12):
                fighter_badges.append('Greasy')
            if (str_def > 63) and (sapm < 2.7):
                fighter_badges.append('Can’t Touch This')
            if (kd_received_avg < 0.15) and (ko_loss_rate < 10):
                fighter_badges.append('Iron Chin')
            if (sub_att_received_avg < 0.3) and (sub_def > 85) and (never_submitted == 1):
                fighter_badges.append('Locksmith')
            if total_fights > 0 and (total_fight_time_sec / total_fights > 450) and (sig_str_landed_per_sec > 0.3):
                fighter_badges.append('The Dogwalker')
            if (five_round_fights >= 2) and (five_round_win_rate > 60) and (five_round_decision_rate > 40) and (five_round_wins >= 1):
                fighter_badges.append('Champ Rounds')

            badges.append({
                'id': fid,
                'badges': ','.join(fighter_badges) if fighter_badges else None
            })

        logger.info(f"Total Yes, Chef badges awarded: {yes_chef_count}")
        df = pd.DataFrame(badges)
        df.to_csv('data/badges.csv', index=False)
        logger.info(f"Generated badges.csv for {len(fighters_df)} fighters.")
        print(f"Generated badges.csv for {len(fighters_df)} fighters.")
    except Exception as e:
        logger.error(f"Failed to assign badges: {str(e)}")
        raise

assign_badges()