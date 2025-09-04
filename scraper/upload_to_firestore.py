import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import os
import numpy as np

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
            {'file': 'data/fight_details.csv', 'collection': 'fights', 'id_field': 'fight_id'}
        ]
        
        for item in files:
            file_path = item['file']
            collection_name = item['collection']
            id_field = item['id_field']
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            df = pd.read_csv(file_path)
            
            # Upload each row as a document, using the specified ID field
            for _, row in df.iterrows():
                doc_id = str(row[id_field])
                doc_data = row.to_dict()
                # Handle badges: convert to list if string, else empty list
                if 'badges' in doc_data and isinstance(doc_data['badges'], str) and doc_data['badges']:
                    doc_data['badges'] = doc_data['badges'].split(',')
                else:
                    doc_data['badges'] = []
                # Convert NaN values to None for Firestore compatibility
                for key, value in doc_data.items():
                    if isinstance(value, float) and np.isnan(value):
                        doc_data[key] = None
                # Upload to Firestore
                db.collection(collection_name).document(doc_id).set(doc_data)
                print(f"Uploaded document {doc_id} to collection {collection_name}")
            
            print(f"Uploaded {len(df)} documents to collection {collection_name}")
        
        print("Firestore upload complete!")
    
    except Exception as e:
        print(f"Failed to upload to Firestore: {str(e)}")
        raise

if __name__ == '__main__':
    upload_to_firestore()