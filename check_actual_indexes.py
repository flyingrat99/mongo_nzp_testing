from pymongo import MongoClient
import json

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')

# Collections to check in each database
COLLECTIONS = [
    "summary_1_week",
    "summary_2_weeks",
    "summary_1_month",
    "summary_3_months"
]

# Databases to check
DATABASES = [
    "nzpost_summary", 
    "nzpost_summary_item", 
    "nzpost_summary_append"
]

def check_indexes():
    """Check actual indexes in MongoDB collections"""
    for db_name in DATABASES:
        print(f"\n{'=' * 50}")
        print(f"DATABASE: {db_name}")
        print(f"{'=' * 50}")
        
        db = client[db_name]
        
        for collection_name in COLLECTIONS:
            print(f"\n--- COLLECTION: {collection_name} ---")
            
            try:
                collection = db[collection_name]
                indexes = list(collection.list_indexes())
                
                if not indexes:
                    print(f"  No indexes found!")
                    continue
                
                for idx in indexes:
                    name = idx.get('name', 'unknown')
                    key_fields = idx.get('key', {})
                    
                    # Format index fields as key: direction pairs
                    key_str = ", ".join([f"{k}: {v}" for k, v in key_fields.items()])
                    
                    # Extra properties
                    properties = []
                    
                    if idx.get('unique', False):
                        properties.append("unique")
                    
                    if idx.get('sparse', False):
                        properties.append("sparse")
                    
                    if idx.get('background', False):
                        properties.append("background")
                        
                    if 'expireAfterSeconds' in idx:
                        properties.append(f"TTL: {idx['expireAfterSeconds']}s")
                    
                    # Check for special index types
                    if name == "_id_":
                        properties.append("primary key")
                    
                    if idx.get('2dsphereIndexVersion', None):
                        properties.append("2dsphere")
                    
                    if idx.get('textIndexVersion', None):
                        properties.append("text")
                    
                    # Format properties string
                    props_str = f" ({', '.join(properties)})" if properties else ""
                    
                    print(f"  * {name}: {{{key_str}}}{props_str}")
                    
                    # Print detailed index definition for debugging
                    if 'weights' in idx or 'default_language' in idx:
                        print(f"    Details: {json.dumps(idx, default=str)}")
                
            except Exception as e:
                print(f"  Error checking indexes: {str(e)}")

if __name__ == "__main__":
    check_indexes() 