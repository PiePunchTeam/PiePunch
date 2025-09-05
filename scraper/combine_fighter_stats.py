import pandas as pd
import os
import logging

# Setup logging
logging.basicConfig(filename='combine_stats_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('combine_fighter_stats')

def combine_fighter_stats():
    try:
        # Load input CSVs
        fighter_file = 'data/fighter_details.csv'
        defensive_file = 'data/defensive_stats.csv'
        derived_file = 'data/derived_stats.csv'
        
        if not os.path.exists(fighter_file):
            logger.error(f"{fighter_file} not found")
            print(f"Error: {fighter_file} not found")
            return
        fighters_df = pd.read_csv(fighter_file)
        logger.info(f"Loaded {fighter_file} with {len(fighters_df)} fighters")
        
        if not os.path.exists(defensive_file):
            logger.warning(f"{defensive_file} not found, creating empty DataFrame")
            print(f"Warning: {defensive_file} not found, proceeding with partial data")
            defensive_df = pd.DataFrame(columns=['id'])
        else:
            defensive_df = pd.read_csv(defensive_file)
            logger.info(f"Loaded {defensive_file} with {len(defensive_df)} records")
        
        if not os.path.exists(derived_file):
            logger.warning(f"{derived_file} not found, creating empty DataFrame")
            print(f"Warning: {derived_file} not found, proceeding with partial data")
            derived_df = pd.DataFrame(columns=['id'])
        else:
            derived_df = pd.read_csv(derived_file)
            logger.info(f"Loaded {derived_file} with {len(derived_df)} records")
        
        # Ensure 'id' is string for consistent merging
        fighters_df['id'] = fighters_df['id'].astype(str)
        defensive_df['id'] = defensive_df['id'].astype(str)
        derived_df['id'] = derived_df['id'].astype(str)
        
        # Merge with left joins to retain all fighters
        combined_df = fighters_df.merge(defensive_df, on='id', how='left')
        combined_df = combined_df.merge(derived_df, on='id', how='left')
        
        # Fill NaN values with defaults (0 for numeric, empty string for others)
        numeric_columns = combined_df.select_dtypes(include=['float64', 'int64']).columns
        combined_df[numeric_columns] = combined_df[numeric_columns].fillna(0)
        combined_df = combined_df.fillna('')
        
        # Save to CSV
        output_file = 'data/fighters_stats.csv'
        combined_df.to_csv(output_file, index=False)
        logger.info(f"Generated {output_file} for {len(combined_df)} fighters")
        print(f"Generated {output_file} for {len(combined_df)} fighters")
    
    except Exception as e:
        logger.error(f"Failed to combine fighter stats: {str(e)}")
        print(f"Failed to combine fighter stats: {str(e)}")
        raise

if __name__ == '__main__':
    combine_fighter_stats()