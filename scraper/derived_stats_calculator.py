import pandas as pd
import numpy as np
import logging

logging.basicConfig(filename='derived_stats_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('derived_stats')

def calculate_derived_stats():
    try:
        fight_df = pd.read_csv('data/fight_details.csv')
        event_df = pd.read_csv('data/event_details.csv')
        fight_df = fight_df.merge(event_df[['fight_id', 'winner_id']], on='fight_id', how='left')
        fighter_ids = pd.concat([fight_df['r_id'], fight_df['b_id']]).unique()
        derived_stats = []

        for fid in fighter_ids:
            r_fights = fight_df[fight_df['r_id'] == fid]
            b_fights = fight_df[fight_df['b_id'] == fid]
            total_fights = len(r_fights) + len(b_fights)
            total_fight_time_sec = 0
            ko_tko_wins = sub_wins = 0
            five_round_fights = five_round_wins = five_round_decision_wins = 0
            ko_losses = sub_losses = 0
            ground_ko_tko_wins = ground_landed_tko = 0
            ctrl_time = kd = strikes_attempted = sub_att = 0
            leg_landed = body_landed = td_landed = td_attempted = 0
            sig_str_landed_total = 0
            splm_per_fight = []

            for _, row in r_fights.iterrows():
                total_fight_time_sec += row['match_time_sec']
                if row['winner_id'] == fid:
                    if 'ko' in row['method'].lower():
                        ko_tko_wins += 1
                        if row['r_ground_landed'] > 0:
                            ground_ko_tko_wins += 1
                            ground_landed_tko += row['r_ground_landed']
                    if 'submission' in row['method'].lower():
                        sub_wins += 1
                    if row['total_rounds'] == 5:
                        five_round_fights += 1
                        five_round_wins += 1
                        if 'decision' in row['method'].lower():
                            five_round_decision_wins += 1
                else:
                    if 'ko' in row['method'].lower():
                        ko_losses += 1
                    if 'submission' in row['method'].lower():
                        sub_losses += 1
                ctrl_time += row['r_ctrl'] if pd.notnull(row['r_ctrl']) else 0
                kd += row['r_kd'] if pd.notnull(row['r_kd']) else 0
                strikes_attempted += row['r_sig_str_atmpted'] if pd.notnull(row['r_sig_str_atmpted']) else 0
                sub_att += row['r_sub_att'] if pd.notnull(row['r_sub_att']) else 0
                leg_landed += row['r_leg_landed'] if pd.notnull(row['r_leg_landed']) else 0
                body_landed += row['r_body_landed'] if pd.notnull(row['r_body_landed']) else 0
                td_landed += row['r_td_landed'] if pd.notnull(row['r_td_landed']) else 0
                td_attempted += row['r_td_atmpted'] if pd.notnull(row['r_td_atmpted']) else 0
                sig_str_landed = row['r_sig_str_landed'] if pd.notnull(row['r_sig_str_landed']) else 0
                sig_str_landed_total += sig_str_landed
                minutes = row['match_time_sec'] / 60
                splm = (sig_str_landed / minutes) if minutes > 0 else 0
                splm_per_fight.append(splm)

            for _, row in b_fights.iterrows():
                total_fight_time_sec += row['match_time_sec']
                if row['winner_id'] == fid:
                    if 'ko' in row['method'].lower():
                        ko_tko_wins += 1
                        if row['b_ground_landed'] > 0:
                            ground_ko_tko_wins += 1
                            ground_landed_tko += row['b_ground_landed']
                    if 'submission' in row['method'].lower():
                        sub_wins += 1
                    if row['total_rounds'] == 5:
                        five_round_fights += 1
                        five_round_wins += 1
                        if 'decision' in row['method'].lower():
                            five_round_decision_wins += 1
                else:
                    if 'ko' in row['method'].lower():
                        ko_losses += 1
                    if 'submission' in row['method'].lower():
                        sub_losses += 1
                ctrl_time += row['b_ctrl'] if pd.notnull(row['b_ctrl']) else 0
                kd += row['b_kd'] if pd.notnull(row['b_kd']) else 0
                strikes_attempted += row['b_sig_str_atmpted'] if pd.notnull(row['b_sig_str_atmpted']) else 0
                sub_att += row['b_sub_att'] if pd.notnull(row['b_sub_att']) else 0
                leg_landed += row['b_leg_landed'] if pd.notnull(row['b_leg_landed']) else 0
                body_landed += row['b_body_landed'] if pd.notnull(row['b_body_landed']) else 0
                td_landed += row['b_td_landed'] if pd.notnull(row['b_td_landed']) else 0
                td_attempted += row['b_td_atmpted'] if pd.notnull(row['b_td_atmpted']) else 0
                sig_str_landed = row['b_sig_str_landed'] if pd.notnull(row['b_sig_str_landed']) else 0
                sig_str_landed_total += sig_str_landed
                minutes = row['match_time_sec'] / 60
                splm = (sig_str_landed / minutes) if minutes > 0 else 0
                splm_per_fight.append(splm)

            if total_fight_time_sec > 0:
                minutes = total_fight_time_sec / 60
                finish_rate = ((ko_tko_wins + sub_wins) / total_fights) * 100
                ctrl_avg = (ctrl_time / minutes) * 15
                leg_landed_avg = (leg_landed / minutes) * 15
                body_landed_avg = (body_landed / minutes) * 15
                career_td_acc = (td_landed / td_attempted * 100) if td_attempted > 0 else 0
                ko_loss_rate = (ko_losses / total_fights) * 100 if total_fights > 0 else 0
                five_round_win_rate = (five_round_wins / five_round_fights * 100) if five_round_fights > 0 else 0
                five_round_decision_rate = (five_round_decision_wins / five_round_wins * 100) if five_round_wins > 0 else 0
                ground_finish_rate = (ground_ko_tko_wins / ko_tko_wins * 100) if ko_tko_wins > 0 else 0
                ground_landed_per_tko = (ground_landed_tko / ko_tko_wins) if ko_tko_wins > 0 else 0
                sig_str_landed_per_sec = sig_str_landed_total / total_fight_time_sec if total_fight_time_sec > 0 else 0
                splm_std = np.std(splm_per_fight) if len(splm_per_fight) > 0 else 0
            else:
                finish_rate = ctrl_avg = leg_landed_avg = body_landed_avg = career_td_acc = 0
                ko_loss_rate = five_round_win_rate = five_round_decision_rate = ground_finish_rate = 0
                ground_landed_per_tko = sig_str_landed_per_sec = splm_std = 0

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
                'career_td_acc': round(career_td_acc, 2),
                'ko_tko_wins': ko_tko_wins,
                'sub_wins': sub_wins,
                'ko_loss_rate': round(ko_loss_rate, 2),
                'never_submitted': 1 if sub_losses == 0 else 0,
                'five_round_fights': five_round_fights,
                'five_round_wins': five_round_wins,
                'five_round_win_rate': round(five_round_win_rate, 2),
                'five_round_decision_rate': round(five_round_decision_rate, 2),
                'ground_finish_rate': round(ground_finish_rate, 2),
                'ground_landed_per_tko': round(ground_landed_per_tko, 2),
                'sig_str_landed_per_sec': round(sig_str_landed_per_sec, 4),
                'splm_std': round(splm_std, 2)
            })

        df = pd.DataFrame(derived_stats)
        df.to_csv('data/derived_stats.csv', index=False)
        logger.info(f"Generated derived_stats.csv for {len(fighter_ids)} fighters.")
        print(f"Generated derived_stats.csv for {len(fighter_ids)} fighters.")
    except Exception as e:
        logger.error(f"Failed to calculate derived stats: {str(e)}")
        raise

calculate_derived_stats()