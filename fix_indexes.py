from pymongo import MongoClient, ASCENDING
import json

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')

# Index configurations
INDEX_CONFIG = {
    "nzpost_summary": [
        [("tpid", 1)],
        [("edifact_code", 1)],
        [("event_datetime", 1)],
        [("tpid", 1), ("edifact_code", 1)],
        [("tpid", 1), ("event_datetime", 1)],
        [("edifact_code", 1), ("event_datetime", 1)],
        [("tpid", 1), ("edifact_code", 1), ("event_datetime", 1)]
    ],
    "nzpost_summary_item": [
        [("tpid", 1)],
        [("edifact_code", 1)],
        [("event_datetime", 1)],
        [("tpid", 1), ("edifact_code", 1)],
        [("tpid", 1), ("event_datetime", 1)],
        [("edifact_code", 1), ("event_datetime", 1)],
        [("tpid", 1), ("edifact_code", 1), ("event_datetime", 1)]
    ],
    "nzpost_summary_append": [
        [("tpid", 1)],
        [("tracking_reference", 1)],
        [("edifact_code", 1)],
        [("timestamp", 1)],
        [("tpid", 1), ("timestamp", 1)],
        [("tpid", 1), ("edifact_code", 1)]
    ]
}

# Collections in each database
COLLECTIONS = [
    "summary_1_week",
    "summary_2_weeks",
    "summary_1_month",
    "summary_3_months"
]

def fix_indexes():
    """Fix indexes for all databases and collections"""
    for db_name, indexes in INDEX_CONFIG.items():
        print(f"\n=== Fixing indexes for {db_name} ===")
        db = client[db_name]
        
        for collection_name in COLLECTIONS:
            print(f"\nCollection: {collection_name}")
            collection = db[collection_name]
            
            try:
                # Drop all existing indexes except _id
                print("Dropping existing indexes...")
                collection.drop_indexes()
                
                # Create new indexes
                print("Creating new indexes...")
                for index in indexes:
                    index_name = "_".join(f"{field}_{direction}" for field, direction in index)
                    print(f"  Creating {index_name}")
                    collection.create_index(index, name=index_name)
                
                print("Indexes updated successfully")
                
            except Exception as e:
                print(f"Error fixing indexes: {str(e)}")

if __name__ == "__main__":
    fix_indexes() 