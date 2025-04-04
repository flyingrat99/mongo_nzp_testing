import pymongo
from pymongo import MongoClient, ASCENDING, DESCENDING
import time

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['nzpost_summary']

# Collection names
COLLECTIONS = [
    "summary_1_week",
    "summary_2_weeks",
    "summary_1_month",
    "summary_3_months"
]

def create_indexes():
    print("Creating indexes for MongoDB collections...")
    
    for collection_name in COLLECTIONS:
        collection = db[collection_name]
        
        # Drop existing indexes (except _id)
        print(f"Dropping existing indexes for {collection_name}...")
        collection.drop_indexes()
        
        # Create single-field indexes
        print(f"Creating single-field indexes for {collection_name}...")
        collection.create_index([("tpid", ASCENDING)], name="tpid_1")
        collection.create_index([("edifact_code", ASCENDING)], name="edifact_code_1")
        collection.create_index([("event_datetime", ASCENDING)], name="event_datetime_1")
        collection.create_index([("tracking_reference", ASCENDING)], name="tracking_reference_1")
        
        # Create compound indexes for common query patterns
        print(f"Creating compound indexes for {collection_name}...")
        
        # Index for TPID + edifact_code (most common query pattern)
        collection.create_index(
            [("tpid", ASCENDING), ("edifact_code", ASCENDING)],
            name="tpid_1_edifact_code_1"
        )
        
        # Index for TPID + event_datetime
        collection.create_index(
            [("tpid", ASCENDING), ("event_datetime", ASCENDING)],
            name="tpid_1_event_datetime_1"
        )
        
        # Index for edifact_code + event_datetime
        collection.create_index(
            [("edifact_code", ASCENDING), ("event_datetime", ASCENDING)],
            name="edifact_code_1_event_datetime_1"
        )
        
        # Index for TPID + edifact_code + event_datetime (comprehensive query pattern)
        collection.create_index(
            [("tpid", ASCENDING), ("edifact_code", ASCENDING), ("event_datetime", ASCENDING)],
            name="tpid_1_edifact_code_1_event_datetime_1"
        )
        
        # List all indexes
        indexes = collection.list_indexes()
        print(f"Indexes for {collection_name}:")
        for index in indexes:
            print(f"  - {index['name']}: {index['key']}")
        
        print(f"Completed indexing for {collection_name}\n")
    
    print("All indexes created successfully!")

def test_query_performance():
    print("\nTesting query performance...")
    
    # Test query for TPID + edifact_code
    collection = db["summary_1_week"]
    query = {
        "tpid": {"$in": [1000011, 1000012]},
        "edifact_code": 500
    }
    
    # Test with aggregation pipeline
    start_time = time.time()
    pipeline = [
        {"$match": query},
        {"$count": "total"}
    ]
    result = list(collection.aggregate(pipeline, allowDiskUse=False))
    end_time = time.time()
    
    count = result[0]["total"] if result else 0
    print(f"Test query result: {count} documents")
    print(f"Query execution time: {(end_time - start_time) * 1000:.2f}ms")

if __name__ == "__main__":
    create_indexes()
    test_query_performance() 