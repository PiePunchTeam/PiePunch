import pandas as pd
import os
import csv

def generate_comprehensive_fighter_details():
    try:
        fighters_file = 'data/fighters_stats.csv'
        badges_file = 'data/badges.csv'
        
        if not os.path.exists(fighters_file) or not os.path.exists(badges_file):
            raise FileNotFoundError("Required files not found: fighters_stats.csv or badges.csv")
        
        fighters_df = pd.read_csv(fighters_file)
        badges_df = pd.read_csv(badges_file)[['id', 'badges']]
        
        # Merge on 'id', keeping all fighters (left join)
        comprehensive_df = fighters_df.merge(badges_df, on='id', how='left')
        
        # Replace NaN in 'badges' with empty string for fighters with no badges
        comprehensive_df['badges'] = comprehensive_df['badges'].fillna('')
        
        # Save to CSV
        output_file = 'data/comprehensive_fighter_details.csv'
        comprehensive_df.to_csv(output_file, quoting=csv.QUOTE_ALL, index=False)
        print(f"Generated {output_file} with {len(comprehensive_df)} fighters.")
    
    except Exception as e:
        print(f"Failed to generate comprehensive fighter details: {str(e)}")
        raise

if __name__ == '__main__':
    generate_comprehensive_fighter_details()