import pymongo
from pymongo import MongoClient
from datetime import datetime, timedelta
import random
import logging
from tqdm import tqdm
import concurrent.futures
from typing import List, Dict, Tuple
import math

# Configure logging
logging.basicConfig(
    filename='error.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['nzpost_summary']

# Collection configurations
COLLECTIONS = {
    'summary_1_week': {
        'days': 7,
        'total_parcels': 3_000_000,
        'start_date': datetime(2025, 3, 1),
        'end_date': datetime(2025, 3, 7)
    },
    'summary_2_weeks': {
        'days': 14,
        'total_parcels': 6_000_000,
        'start_date': datetime(2025, 3, 1),
        'end_date': datetime(2025, 3, 14)
    },
    'summary_1_month': {
        'days': 31,
        'total_parcels': 12_900_000,
        'start_date': datetime(2025, 3, 1),
        'end_date': datetime(2025, 3, 31)
    },
    'summary_3_months': {
        'days': 90,
        'total_parcels': 38_600_000,
        'start_date': datetime(2025, 1, 1),
        'end_date': datetime(2025, 3, 31)
    }
}

# TPID configurations
TPID_CONFIGS = [
    (1000011, 500000),
    (1000012, 300000),
    (1000013, 200000),
    (1000014, 100000),
    (1000015, 75000),
    (1000016, 50000),
    (1000017, 25000),
    (1000018, 10000),
    (1000019, 5000),
    (1000020, 2000)
]

# Edifact codes and their ratios
EDIFACT_CODES = [
    (100, "Picked Up", 5),
    (200, "In Transit", 15),
    (300, "In Depot", 15),
    (400, "Out for Delivery", 10),
    (500, "Delivered", 50),
    (600, "Attempted Delivery", 5)
]

def generate_tracking_number(base_number: int) -> str:
    return f"NZ{base_number:09d}"

def get_random_edifact_code() -> Tuple[int, str]:
    total = sum(ratio for _, _, ratio in EDIFACT_CODES)
    r = random.uniform(0, total)
    upto = 0
    for code, desc, ratio in EDIFACT_CODES:
        if upto + ratio >= r:
            return code, desc
        upto += ratio
    return EDIFACT_CODES[-1][0], EDIFACT_CODES[-1][1]

def generate_random_datetime(start_date: datetime, end_date: datetime) -> datetime:
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + timedelta(days=random_number_of_days)
    random_hour = random.randint(0, 23)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)
    return random_date.replace(hour=random_hour, minute=random_minute, second=random_second)

def generate_documents_batch(
    start_tracking: int,
    batch_size: int,
    tpid: int,
    start_date: datetime,
    end_date: datetime
) -> List[Dict]:
    documents = []
    for i in range(batch_size):
        tracking_number = generate_tracking_number(start_tracking + i)
        edifact_code, event_description = get_random_edifact_code()
        event_datetime = generate_random_datetime(start_date, end_date)
        
        document = {
            "tracking_reference": tracking_number,
            "tpid": tpid,
            "edifact_code": edifact_code,
            "event_description": event_description,
            "event_datetime": event_datetime
        }
        documents.append(document)
    return documents

def process_collection(collection_name: str, config: Dict):
    try:
        collection = db[collection_name]
        
        # Drop collection if exists
        collection.drop()
        
        # Create indexes
        collection.create_index([("tpid", 1)])
        collection.create_index([("edifact_code", 1)])
        collection.create_index([("event_datetime", 1)])
        collection.create_index([("tracking_reference", 1)])
        
        # Calculate TPID distribution
        total_monthly_parcels = config['total_parcels']
        days_in_month = 31  # March has 31 days
        monthly_scale = days_in_month / config['days']
        
        # Process fixed TPIDs
        tracking_counter = 100000001
        batch_size = 10000
        
        for tpid, monthly_volume in TPID_CONFIGS:
            scaled_volume = int(monthly_volume / monthly_scale)
            num_batches = math.ceil(scaled_volume / batch_size)
            
            with tqdm(total=scaled_volume, desc=f"{collection_name} - TPID {tpid}") as pbar:
                for batch_num in range(num_batches):
                    current_batch_size = min(batch_size, scaled_volume - (batch_num * batch_size))
                    documents = generate_documents_batch(
                        tracking_counter,
                        current_batch_size,
                        tpid,
                        config['start_date'],
                        config['end_date']
                    )
                    collection.insert_many(documents)
                    tracking_counter += current_batch_size
                    pbar.update(current_batch_size)
        
        # Process remaining TPIDs
        remaining_parcels = total_monthly_parcels - sum(
            int(volume / monthly_scale) for _, volume in TPID_CONFIGS
        )
        remaining_tpids = math.ceil(remaining_parcels / 50000)
        
        for tpid in range(2000011, 2000011 + remaining_tpids):
            scaled_volume = 50000
            num_batches = math.ceil(scaled_volume / batch_size)
            
            with tqdm(total=scaled_volume, desc=f"{collection_name} - TPID {tpid}") as pbar:
                for batch_num in range(num_batches):
                    current_batch_size = min(batch_size, scaled_volume - (batch_num * batch_size))
                    documents = generate_documents_batch(
                        tracking_counter,
                        current_batch_size,
                        tpid,
                        config['start_date'],
                        config['end_date']
                    )
                    collection.insert_many(documents)
                    tracking_counter += current_batch_size
                    pbar.update(current_batch_size)
    
    except Exception as e:
        logging.error(f"Error processing collection {collection_name}: {str(e)}")
        raise

def main():
    try:
        # Drop database if exists
        client.drop_database('nzpost_summary')
        
        # Process collections sequentially
        for collection_name, config in COLLECTIONS.items():
            process_collection(collection_name, config)
            
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()

