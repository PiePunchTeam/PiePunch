import pandas as pd
import logging

# Setup logging
logging.basicConfig(filename='derived_stats_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('derived_stats')

def calculate_derived_stats():
    try:
        fight_df = pd.read_csv('data/fight_details.csv')
        event_df = pd.read_csv('data/event_details.csv')
        # Merge on fight_id to get winner_id
        fight_df = fight_df.merge(event_df[['fight_id', 'winner_id']], on='fight_id', how='left')
        fighter_ids = pd.concat([fight_df['r_id'], fight_df['b_id']]).unique()
        derived_stats = []

        for fid in fighter_ids:
            r_fights = fight_df[fight_df['r_id'] == fid]
            b_fights = fight_df[fight_df['b_id'] == fid]
            total_fights = len(r_fights) + len(b_fights)
            total_fight_time_sec = 0
            ko_tko_wins = sub_wins = 0
            five_round_fights = five_round_wins = 0
            ctrl_time = kd = strikes_attempted = sub_att = 0
            leg_landed = body_landed = 0
            td_landed = td_attempted = 0

            for _, row in r_fights.iterrows():
                total_fight_time_sec += row['match_time_sec']
                if row['winner_id'] == fid and 'ko' in row['method'].lower():
                    ko_tko_wins += 1
                if row['winner_id'] == fid and 'submission' in row['method'].lower():
                    sub_wins += 1
                if row['total_rounds'] == 5:
                    five_round_fights += 1
                    if row['winner_id'] == fid:
                        five_round_wins += 1
                ctrl_time += row['r_ctrl'] or 0
                kd += row['r_kd']
                strikes_attempted += row['r_sig_str_atmpted']
                sub_att += row['r_sub_att']
                leg_landed += row['r_leg_landed']
                body_landed += row['r_body_landed']
                td_landed += row['r_td_landed']
                td_attempted += row['r_td_atmpted']

            for _, row in b_fights.iterrows():
                total_fight_time_sec += row['match_time_sec']
                if row['winner_id'] == fid and 'ko' in row['method'].lower():
                    ko_tko_wins += 1
                if row['winner_id'] == fid and 'submission' in row['method'].lower():
                    sub_wins += 1
                if row['total_rounds'] == 5:
                    five_round_fights += 1
                    if row['winner_id'] == fid:
                        five_round_wins += 1
                ctrl_time += row['b_ctrl'] or 0
                kd += row['b_kd']
                strikes_attempted += row['b_sig_str_atmpted']
                sub_att += row['b_sub_att']
                leg_landed += row['b_leg_landed']
                body_landed += row['b_body_landed']
                td_landed += row['b_td_landed']
                td_attempted += row['b_td_atmpted']

            if total_fight_time_sec > 0:
                minutes = total_fight_time_sec / 60
                finish_rate = ((ko_tko_wins + sub_wins) / total_fights) * 100
                ctrl_avg = (ctrl_time / minutes) * 15
                leg_landed_avg = (leg_landed / minutes) * 15
                body_landed_avg = (body_landed / minutes) * 15
            else:
                finish_rate = ctrl_avg = leg_landed_avg = body_landed_avg = 0

            career_td_acc = (td_landed / td_attempted * 100) if td_attempted > 0 else 0

            derived_stats.append({
                'id': fid,
                'total_fights': total_fights,
                'total_fight_time_sec': total_fight_time_sec,
                'finish_rate': round(finish_rate, 2),
                'ctrl_avg': round(ctrl_avg, 2),
                'leg_landed_avg': round(leg_landed_avg, 2),
                'body_landed_avg': round(body_landed_avg, 2),
                'kd': kd,
                'strikes_attempted': strikes_attempted,
                'sub_att': sub_att,
                'career_td_acc': round(career_td_acc, 2)
            })

        df = pd.DataFrame(derived_stats)
        df.to_csv('data/derived_stats.csv', index=False)
        logger.info(f"Generated derived_stats.csv for {len(fighter_ids)} fighters.")
        print(f"Generated derived_stats.csv for {len(fighter_ids)} fighters.")
    except Exception as e:
        logger.error(f"Failed to calculate derived stats: {str(e)}")
        raise

calculate_derived_stats()