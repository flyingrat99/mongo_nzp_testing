from pymongo import MongoClient

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')

# Check each database and collection
databases = ["nzpost_summary", "nzpost_summary_item", "nzpost_summary_append"]
collections = {
    "1 week (3M) - 1st - 7th March": "summary_1_week",
    "2 weeks (6M) - 1st - 14th March": "summary_2_weeks",
    "1 month (13M) - March": "summary_1_month",
    "3 months (38.6M) - Jan-Mar": "summary_3_months"
}

for db_name in databases:
    print(f"\nChecking database: {db_name}")
    db = client[db_name]
    for display_name, coll_name in collections.items():
        count = db[coll_name].count_documents({})
        print(f"Collection {display_name}: {count:,} documents") 