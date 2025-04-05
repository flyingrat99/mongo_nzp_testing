#!/usr/bin/env python3
"""
MongoDB Index Creator for NZ Post Databases

This script creates the necessary indexes for:
1. nzpost_summary - Regular collections 
2. nzpost_summary_item - Item detail collections
3. nzpost_summary_append - Time series collections

All collections in each database will receive the appropriate indexes.
"""

import pymongo
import time
from datetime import datetime

# MongoDB connection parameters
MONGO_URI = "mongodb://localhost:27017/"

# Collection names
COLLECTIONS = [
    "summary_1_week",
    "summary_2_weeks", 
    "summary_1_month",
    "summary_3_months"
]

def create_indexes():
    """Create all required indexes for the NZ Post MongoDB databases"""
    
    client = pymongo.MongoClient(MONGO_URI)
    print(f"Connected to MongoDB at {MONGO_URI}")
    
    # Start time tracking
    start_time = time.time()
    
    # Create indexes for regular collections (nzpost_summary and nzpost_summary_item)
    create_regular_indexes(client, "nzpost_summary")
    create_regular_indexes(client, "nzpost_summary_item")
    
    # Create indexes for time series collections (nzpost_summary_append)
    create_timeseries_indexes(client, "nzpost_summary_append")
    
    # Print completion time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nIndex creation completed in {execution_time:.2f} seconds")
    
    client.close()

def create_regular_indexes(client, db_name):
    """Create indexes for regular collections (nzpost_summary and nzpost_summary_item)"""
    
    print(f"\nCreating indexes for {db_name}...")
    db = client[db_name]
    
    # Regular collection indexes
    regular_indexes = [
        ("tpid_1_edifact_code_1_event_datetime_1", [("tpid", 1), ("edifact_code", 1), ("event_datetime", 1)]),
        ("tpid_1_edifact_code_1", [("tpid", 1), ("edifact_code", 1)]),
        ("tpid_1_tracking_reference_1", [("tpid", 1), ("tracking_reference", 1)]),
        ("tpid_1", [("tpid", 1)]),
        ("event_datetime_1", [("event_datetime", 1)]),
        ("tracking_reference_1", [("tracking_reference", 1)])
    ]
    
    # Create indexes for each collection
    for collection_name in COLLECTIONS:
        collection = db[collection_name]
        
        print(f"  Creating indexes for {collection_name}...")
        for index_name, index_fields in regular_indexes:
            print(f"    - Creating index: {index_name}")
            collection.create_index(index_fields, name=index_name)
        
        # Verify indexes were created
        index_info = collection.index_information()
        print(f"  Created {len(index_info) - 1} indexes for {collection_name}")  # -1 for _id index

def create_timeseries_indexes(client, db_name):
    """Create indexes for time series collections (nzpost_summary_append)"""
    
    print(f"\nCreating indexes for time series database {db_name}...")
    db = client[db_name]
    
    # Time series collection indexes
    timeseries_indexes = [
        ("tpid_1", [("tpid", 1)]),
        ("tracking_reference_1", [("tracking_reference", 1)]),
        ("edifact_code_1", [("edifact_code", 1)]),
        ("timestamp_1", [("timestamp", 1)]),
        ("tpid_1_timestamp_1", [("tpid", 1), ("timestamp", 1)]),
        ("tpid_1_edifact_code_1", [("tpid", 1), ("edifact_code", 1)]),
        ("tracking_reference_1_timestamp_1", [("tracking_reference", 1), ("timestamp", 1)])
    ]
    
    # Create indexes for each collection
    for collection_name in COLLECTIONS:
        collection = db[collection_name]
        
        print(f"  Creating indexes for {collection_name}...")
        for index_name, index_fields in timeseries_indexes:
            print(f"    - Creating index: {index_name}")
            collection.create_index(index_fields, name=index_name)
        
        # Verify indexes were created
        index_info = collection.index_information()
        print(f"  Created {len(index_info) - 1} indexes for {collection_name}")  # -1 for _id index

def print_index_summary(client):
    """Print a summary of all created indexes"""
    
    databases = ["nzpost_summary", "nzpost_summary_item", "nzpost_summary_append"]
    
    print("\n===== Index Creation Summary =====")
    for db_name in databases:
        db = client[db_name]
        print(f"\nDatabase: {db_name}")
        
        for collection_name in COLLECTIONS:
            collection = db[collection_name]
            indexes = collection.index_information()
            
            print(f"  Collection: {collection_name}")
            print(f"  Total indexes: {len(indexes)}")
            
            for index_name, index_info in indexes.items():
                if index_name != "_id_":  # Skip the default _id index
                    key_pattern = ", ".join([f"{k[0]}: {k[1]}" for k in index_info["key"]])
                    print(f"    - {index_name}: {key_pattern}")
            
            print("")

if __name__ == "__main__":
    print("MongoDB Index Creator for NZ Post Databases")
    print("===========================================")
    print(f"Script execution started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        create_indexes()
        
        # Uncomment to print detailed index summary
        # client = pymongo.MongoClient(MONGO_URI)
        # print_index_summary(client)
        # client.close()
        
        print("\nScript completed successfully!")
    except Exception as e:
        print(f"\nError: {str(e)}")
        print("\nScript execution failed.") 