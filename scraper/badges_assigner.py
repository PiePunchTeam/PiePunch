import pandas as pd
import os
import csv

def assign_badges():
    try:
        file_path = 'data/fighters_stats.csv'
        abs_file_path = os.path.abspath(file_path)
        if not os.path.exists(abs_file_path):
            raise FileNotFoundError(f"File not found: {abs_file_path}")
        
        fighters_df = pd.read_csv(file_path)
        
        badges = []
        bakers_dozen_count = 0
        russian_bear_count = 0
        pie_thon_count = 0
        doughmaker_count = 0
        ko_creamer_count = 0
        kickin_pot_pie_count = 0
        greasy_count = 0
        cant_touch_this_count = 0
        iron_chin_count = 0
        locksmith_count = 0
        dogwalker_count = 0
        champ_rounds_count = 0

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
                if (ko_tko_ratio > 0.22) and (strikes_attempted > 0 and kd / strikes_attempted > 0.0018) and (splm < 7.0):
                    fighter_badges.append('KO Creamer')
                    ko_creamer_count += 1
                if (splm > 4.7) and (splm_std < 35.0) and (total_fights >= 5):
                    fighter_badges.append('Bakers Dozen')
                    bakers_dozen_count += 1
                if (td_avg > 2.3) and (career_td_acc > 38) and (ctrl_avg > 180) and (total_fights >= 5):
                    fighter_badges.append('Russian Bear')
                    russian_bear_count += 1
                if (sub_wins_ratio > 0.13) and (sub_att / total_fights > 0.46) and (total_fights >= 5):
                    fighter_badges.append('Pie-thon')
                    pie_thon_count += 1
                if (ground_finish_rate > 55) and (ground_landed_per_tko > 15) and (ctrl_avg > 150) and (total_fights >= 5) and (ko_tko_wins > 0):
                    fighter_badges.append('Doughmaker')
                    doughmaker_count += 1
                if ((leg_landed_avg > 24.5) or (body_landed_avg > 25)) and (leg_landed_avg + body_landed_avg > 50) and (ko_tko_wins > 2) and (total_fights >= 5):
                    fighter_badges.append('Kickin’ Pot Pie')
                    kickin_pot_pie_count += 1
            if (td_def > 82) and (td_attempts_received_avg < 10):
                fighter_badges.append('Greasy')
                greasy_count += 1
            if (str_def > 59) and (sapm < 3.1) and (total_fights >= 5):
                fighter_badges.append('Can’t Touch This')
                cant_touch_this_count += 1
            if (kd_received_avg < 0.15) and (ko_loss_rate < 4) and (total_fights >= 5):
                fighter_badges.append('Iron Chin')
                iron_chin_count += 1
            if (sub_att_received_avg * total_fights < 16) and (never_submitted == 1) and (total_fights >= 8):
                fighter_badges.append('Locksmith')
                locksmith_count += 1
            if (total_fights >= 5) and (total_fight_time_sec / total_fights > 160) and (sig_str_landed_per_sec > 0.23):
                fighter_badges.append('The Dogwalker')
                dogwalker_count += 1
            if (five_round_fights >= 1) and (five_round_win_rate > 25) and (five_round_decision_rate > 5) and (five_round_wins >= 1):
                fighter_badges.append('Champ Rounds')
                champ_rounds_count += 1

            badges.append({
                'id': fid,
                'badges': ','.join(fighter_badges) if fighter_badges else None
            })

        df = pd.DataFrame(badges)
        df.to_csv('data/badges.csv', quoting=csv.QUOTE_ALL, index=False)
        print(f"Generated badges.csv for {len(fighters_df)} fighters.")
    except Exception as e:
        print(f"Failed to assign badges: {str(e)}")
        raise

if __name__ == '__main__':
    import csv
    assign_badges()