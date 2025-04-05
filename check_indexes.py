from pymongo import MongoClient
import json

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')

# Expected indexes from our configuration
EXPECTED_INDEXES = {
    "nzpost_summary": [
        {"name": "tpid_1", "fields": [("tpid", 1)]},
        {"name": "edifact_code_1", "fields": [("edifact_code", 1)]},
        {"name": "event_datetime_1", "fields": [("event_datetime", 1)]},
        {"name": "tpid_1_edifact_code_1", "fields": [("tpid", 1), ("edifact_code", 1)]},
        {"name": "tpid_1_event_datetime_1", "fields": [("tpid", 1), ("event_datetime", 1)]},
        {"name": "edifact_code_1_event_datetime_1", "fields": [("edifact_code", 1), ("event_datetime", 1)]},
        {"name": "tpid_1_edifact_code_1_event_datetime_1", "fields": [("tpid", 1), ("edifact_code", 1), ("event_datetime", 1)]}
    ],
    "nzpost_summary_item": [
        {"name": "tpid_1", "fields": [("tpid", 1)]},
        {"name": "edifact_code_1", "fields": [("edifact_code", 1)]},
        {"name": "event_datetime_1", "fields": [("event_datetime", 1)]},
        {"name": "tpid_1_edifact_code_1", "fields": [("tpid", 1), ("edifact_code", 1)]},
        {"name": "tpid_1_event_datetime_1", "fields": [("tpid", 1), ("event_datetime", 1)]},
        {"name": "edifact_code_1_event_datetime_1", "fields": [("edifact_code", 1), ("event_datetime", 1)]},
        {"name": "tpid_1_edifact_code_1_event_datetime_1", "fields": [("tpid", 1), ("edifact_code", 1), ("event_datetime", 1)]}
    ],
    "nzpost_summary_append": [
        {"name": "tpid_1", "fields": [("tpid", 1)]},
        {"name": "tracking_reference_1", "fields": [("tracking_reference", 1)]},
        {"name": "edifact_code_1", "fields": [("edifact_code", 1)]},
        {"name": "timestamp_1", "fields": [("timestamp", 1)]},
        {"name": "tpid_1_timestamp_1", "fields": [("tpid", 1), ("timestamp", 1)]},
        {"name": "tpid_1_edifact_code_1", "fields": [("tpid", 1), ("edifact_code", 1)]}
    ]
}

# Collections to check in each database
COLLECTIONS = {
    "1 week (3M) - 1st - 7th March": "summary_1_week",
    "2 weeks (6M) - 1st - 14th March": "summary_2_weeks",
    "1 month (13M) - March": "summary_1_month",
    "3 months (38.6M) - Jan-Mar": "summary_3_months"
}

def format_index_key(index_info):
    """Format the index key information into a readable string"""
    key_items = []
    for field, direction in index_info["key"].items():
        if field != "_id":  # Skip the default _id index
            key_items.append(f"{field}: {direction}")
    return ", ".join(key_items)

def check_database_indexes():
    """Check indexes for each database and collection"""
    for db_name, expected_indexes in EXPECTED_INDEXES.items():
        print(f"\n=== Checking {db_name} ===")
        db = client[db_name]
        
        # Check each collection
        for display_name, coll_name in COLLECTIONS.items():
            print(f"\nCollection: {coll_name}")
            collection = db[coll_name]
            
            try:
                # Get actual indexes
                actual_indexes = list(collection.list_indexes())
                print("Actual indexes:")
                for idx in actual_indexes:
                    if idx["name"] != "_id_":  # Skip the default _id index
                        print(f"  - {idx['name']}: {format_index_key(idx)}")
                
                # Check for missing expected indexes
                actual_index_names = {idx["name"] for idx in actual_indexes}
                expected_index_names = {idx["name"] for idx in expected_indexes}
                
                missing_indexes = expected_index_names - actual_index_names
                if missing_indexes:
                    print("\nMissing expected indexes:")
                    for idx_name in missing_indexes:
                        expected_idx = next(idx for idx in expected_indexes if idx["name"] == idx_name)
                        print(f"  - {idx_name}: {', '.join(f'{f[0]}: {f[1]}' for f in expected_idx['fields'])}")
                
                # Check for unexpected indexes
                unexpected_indexes = actual_index_names - expected_index_names - {"_id_"}
                if unexpected_indexes:
                    print("\nUnexpected indexes found:")
                    for idx_name in unexpected_indexes:
                        idx = next(idx for idx in actual_indexes if idx["name"] == idx_name)
                        print(f"  - {idx_name}: {format_index_key(idx)}")
                
            except Exception as e:
                print(f"Error checking indexes: {str(e)}")

if __name__ == "__main__":
    check_database_indexes() 