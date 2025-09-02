import pandas as pd
import logging
import os

logging.basicConfig(filename='badge_distribution_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('badge_distribution')

def calculate_badge_distribution():
    try:
        file_path = 'data/badges.csv'
        abs_file_path = os.path.abspath(file_path)
        if not os.path.exists(abs_file_path):
            logger.error(f"File not found: {abs_file_path}")
            raise FileNotFoundError(f"File not found: {abs_file_path}")
        
        logger.info(f"Loading file: {abs_file_path}")
        badges_df = pd.read_csv(file_path)
        logger.info(f"Loaded badges.csv with {len(badges_df)} fighters")

        # Initialize badge counts
        badge_counts = {
            'KO Creamer': 0,
            'Yes, Chef': 0,
            'Russian Bear': 0,
            'Pie-thon': 0,
            'Doughmaker': 0,
            'Kickin’ Pot Pie': 0,
            'Greasy': 0,
            'Can’t Touch This': 0,
            'Iron Chin': 0,
            'Locksmith': 0,
            'The Dogwalker': 0,
            'Champ Rounds': 0
        }
        total_fighters = len(badges_df)

        # Count badge occurrences
        for _, row in badges_df.iterrows():
            badges = str(row['badges']).split(',') if pd.notnull(row['badges']) else []
            for badge in badges:
                badge = badge.strip()
                if badge in badge_counts:
                    badge_counts[badge] += 1

        # Calculate percentages and log results
        distribution = []
        for badge, count in badge_counts.items():
            percentage = (count / total_fighters * 100) if total_fighters > 0 else 0
            distribution.append({
                'Badge': badge,
                'Count': count,
                'Percentage': round(percentage, 2)
            })
            logger.info(f"Badge: {badge}, Count: {count}, Percentage: {percentage:.2f}%")

        # Save distribution to CSV
        df = pd.DataFrame(distribution)
        df.to_csv('data/badge_distribution.csv', index=False)
        logger.info(f"Generated badge_distribution.csv with distribution for {len(badge_counts)} badges")
        print(f"Badge Distribution:")
        print(df)

    except Exception as e:
        logger.error(f"Failed to calculate badge distribution: {str(e)}")
        raise

calculate_badge_distribution()