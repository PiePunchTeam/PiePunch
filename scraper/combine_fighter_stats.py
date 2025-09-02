import pandas as pd
import logging

logging.basicConfig(filename='combine_stats_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('combine_stats')

def combine_fighter_stats():
    try:
        fighter_df = pd.read_csv('data/fighter_details.csv')
        defensive_df = pd.read_csv('data/defensive_stats.csv')
        derived_df = pd.read_csv('data/derived_stats.csv')
        merged_df = fighter_df.merge(defensive_df, on='id', how='left')
        merged_df = merged_df.merge(derived_df, on='id', how='left')
        merged_df = merged_df.rename(columns={'td_avg_acc': 'career_td_acc_ufcstats'})
        merged_df.to_csv('data/fighters_stats.csv', index=False)
        logger.info(f"Generated fighters_stats.csv for {len(merged_df)} fighters.")
        print(f"Generated fighters_stats.csv for {len(merged_df)} fighters.")
    except Exception as e:
        logger.error(f"Failed to combine stats: {str(e)}")
        raise

combine_fighter_stats()