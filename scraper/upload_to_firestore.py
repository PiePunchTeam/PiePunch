import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import os
import numpy as np
import logging

# Setup logging
logging.basicConfig(filename='firestore_upload_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('upload_to_firestore')

def upload_to_firestore():
    try:
        # Path to your service account key JSON file
        cred_path = r'C:\Users\Krega\Documents\PiePunch\config\firebase-adminsdk.json'
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        
        # Files to upload and their collection names
        files = [
            {'file': 'data/comprehensive_fighter_details.csv', 'collection': 'fighters', 'id_field': 'id'},
            {'file': 'data/event_details.csv', 'collection': 'events', 'id_field': 'event_id'},
            {'file': 'data/fight_details.csv', 'collection': 'fights', 'id_field': 'fight_id'},
            {'file': 'data/upcoming_event_details.csv', 'collection': 'upcoming_events', 'id_field': 'event_id'},
            {'file': 'data/upcoming_fight_details.csv', 'collection': 'upcoming_fights', 'id_field': 'fight_id'}
        ]
        
        for item in files:
            file_path = item['file']
            collection_name = item['collection']
            id_field = item['id_field']
            logger.info(f"Processing file: {file_path} for collection: {collection_name}")
            
            if not os.path.exists(file_path):
                logger.warning(f"Skipping {file_path}: File not found")
                print(f"Skipping {file_path}: File not found")
                continue
            
            df = pd.read_csv(file_path)
            updated_count = 0
            new_count = 0
            
            if collection_name in ['events', 'upcoming_events']:
                # Aggregate by event_id
                agg_columns = {
                    'date': 'first',
                    'location': 'first'
                }
                if collection_name == 'events':
                    agg_columns.update({
                        'fight_id': lambda x: list(x),
                        'winner': lambda x: list(x),
                        'winner_id': lambda x: list(x)
                    })
                elif collection_name == 'upcoming_events':
                    agg_columns['event_name'] = 'first'
                df = df.groupby(id_field).agg(agg_columns).reset_index()
            
            # Upload each row as a document
            for _, row in df.iterrows():
                doc_id = str(row[id_field])
                doc_data = row.to_dict()
                # Handle badges for fighters
                if 'badges' in doc_data and isinstance(doc_data['badges'], str) and doc_data['badges']:
                    doc_data['badges'] = doc_data['badges'].split(',')
                else:
                    doc_data['badges'] = []
                # Convert NaN values to None
                for key, value in doc_data.items():
                    if isinstance(value, float) and np.isnan(value):
                        doc_data[key] = None
                
                # Check if document exists in Firestore
                doc_ref = db.collection(collection_name).document(doc_id)
                existing_doc = doc_ref.get()
                
                if existing_doc.exists:
                    existing_data = existing_doc.to_dict()
                    # Convert badges to list for comparison
                    if 'badges' in existing_data and isinstance(existing_data['badges'], str):
                        existing_data['badges'] = existing_data['badges'].split(',') if existing_data['badges'] else []
                    # For events, ensure lists are compared correctly
                    if collection_name in ['events', 'upcoming_events']:
                        for key in ['fight_id', 'winner', 'winner_id']:
                            if key in existing_data and isinstance(existing_data[key], str):
                                existing_data[key] = [existing_data[key]] if existing_data[key] else []
                    if existing_data == doc_data:
                        logger.info(f"Skipping unchanged document {doc_id} in {collection_name}")
                        continue
                    else:
                        doc_ref.set(doc_data)
                        updated_count += 1
                        logger.info(f"Updated document {doc_id} in {collection_name}")
                        print(f"Updated document {doc_id} in {collection_name}")
                else:
                    doc_ref.set(doc_data)
                    new_count += 1
                    logger.info(f"Added new document {doc_id} in {collection_name}")
                    print(f"Added new document {doc_id} in {collection_name}")
            
            logger.info(f"Uploaded {new_count} new and {updated_count} updated documents to {collection_name}")
            print(f"Uploaded {new_count} new and {updated_count} updated documents to {collection_name}")
        
        logger.info("Firestore upload complete!")
        print("Firestore upload complete!")
    
    except Exception as e:
        logger.error(f"Failed to upload to Firestore: {str(e)}")
        print(f"Failed to upload to Firestore: {str(e)}")
        raise

if __name__ == '__main__':
    upload_to_firestore()