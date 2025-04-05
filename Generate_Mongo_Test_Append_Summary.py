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
db = client['nzpost_summary_append']  # New database name

# Collection configurations
COLLECTIONS = {
    'summary_1_week': {
        'start_date': datetime(2025, 3, 1),
        'end_date': datetime(2025, 3, 7),
        'days': 7,
        'total_parcels': 3000000  # Fixed: 3M parcels
    },
    'summary_2_weeks': {
        'start_date': datetime(2025, 3, 1),
        'end_date': datetime(2025, 3, 14),
        'days': 14,
        'total_parcels': 6000000  # Fixed: 6M parcels
    },
    'summary_1_month': {
        'start_date': datetime(2025, 3, 1),
        'end_date': datetime(2025, 3, 31),
        'days': 31,
        'total_parcels': 9000000  # Fixed: 9M parcels
    },
    'summary_3_months': {
        'start_date': datetime(2025, 3, 1),
        'end_date': datetime(2025, 5, 31),
        'days': 92,
        'total_parcels': 27000000  # Fixed: 27M parcels
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

def generate_random_datetime(start_date: datetime, end_date: datetime) -> datetime:
    """Generate a random datetime between start_date and end_date"""
    # Ensure start_date is before end_date
    if start_date >= end_date:
        return start_date  # Return start_date if range is invalid
        
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    seconds_between_dates = time_between_dates.seconds
    
    # Generate random number of seconds within the range
    random_seconds = random.randint(0, days_between_dates * 86400 + seconds_between_dates)
    
    # Add random seconds to start_date
    return start_date + timedelta(seconds=random_seconds)

def generate_event_sequence(start_date: datetime, end_date: datetime) -> List[Dict]:
    """Generate a sequence of events for a parcel"""
    events = []
    current_date = start_date
    
    # Always start with Picked Up
    events.append({
        "edifact_code": 100,
        "event_description": "Picked Up",
        "event_datetime": current_date
    })
    
    # Determine if this parcel will be delivered
    will_be_delivered = random.random() < 0.95  # 95% chance of being delivered
    
    # Generate intermediate events
    num_events = random.randint(2, 20)  # Total events including Picked Up and possibly Delivered
    remaining_events = num_events - 1  # Subtract 1 for Picked Up
    
    if will_be_delivered:
        remaining_events -= 1  # Subtract 1 for Delivered
    
    # Generate intermediate events
    for _ in range(remaining_events):
        # Choose event type based on current state
        if events[-1]["edifact_code"] == 100:  # After Picked Up
            next_code = 200  # In Transit
        elif events[-1]["edifact_code"] == 200:  # After In Transit
            next_code = random.choice([200, 300])  # Another In Transit or In Depot
        elif events[-1]["edifact_code"] == 300:  # After In Depot
            next_code = random.choice([200, 300, 400])  # In Transit, In Depot, or Out for Delivery
        elif events[-1]["edifact_code"] == 400:  # After Out for Delivery
            if random.random() < 0.1:  # 10% chance of Attempted Delivery
                next_code = 600
            else:
                next_code = random.choice([200, 300, 400])  # Back to In Transit, In Depot, or another Out for Delivery
        elif events[-1]["edifact_code"] == 600:  # After Attempted Delivery
            next_code = 400  # Back to Out for Delivery
        else:
            next_code = random.choice([200, 300, 400])  # Default to intermediate states
        
        # Generate event
        event_description = next(code[1] for code in EDIFACT_CODES if code[0] == next_code)
        
        # Ensure we don't generate dates beyond end_date
        max_date = min(current_date + timedelta(days=1), end_date)
        current_date = generate_random_datetime(current_date, max_date)
        
        events.append({
            "edifact_code": next_code,
            "event_description": event_description,
            "event_datetime": current_date
        })
    
    # Add Delivered event if applicable
    if will_be_delivered:
        # Ensure delivery date is within range
        max_date = min(current_date + timedelta(days=1), end_date)
        current_date = generate_random_datetime(current_date, max_date)
        events.append({
            "edifact_code": 500,
            "event_description": "Delivered",
            "event_datetime": current_date
        })
    
    return events

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
        events = generate_event_sequence(start_date, end_date)
        
        # Get the last event's datetime for the time series field
        last_event = events[-1]
        
        # Create a document for each event
        for event in events:
            document = {
                "tracking_reference": tracking_number,
                "tpid": tpid,
                "timestamp": event["event_datetime"],  # Each event becomes a separate document
                "edifact_code": event["edifact_code"],
                "event_description": event["event_description"]
            }
            documents.append(document)
    return documents

def get_db_connection():
    """Create a new MongoDB connection"""
    client = MongoClient('mongodb://localhost:27017/')
    return client['nzpost_summary_append']

def process_tpid_batch(args):
    """Process a single batch for a TPID"""
    tracking_counter, batch_size, tpid, start_date, end_date, collection_name = args
    try:
        db = get_db_connection()
        collection = db[collection_name]
        
        # Process in larger chunks to improve performance
        chunk_size = 50000  # Increased from 10000 to 50000
        documents = []
        
        # Generate all documents first
        for i in range(0, batch_size, chunk_size):
            current_chunk_size = min(chunk_size, batch_size - i)
            chunk_docs = generate_documents_batch(
                tracking_counter + i,
                current_chunk_size,
                tpid,
                start_date,
                end_date
            )
            documents.extend(chunk_docs)
        
        # Insert in one large batch
        if documents:
            collection.insert_many(documents, ordered=False)  # Use unordered inserts for better performance
        
        return batch_size
    except Exception as e:
        logging.error(f"Error in batch processing: {str(e)}")
        raise

def process_tpid(tpid_info, collection_name, config):
    """Process a single TPID in parallel batches"""
    tpid, monthly_volume = tpid_info
    total_monthly_parcels = config['total_parcels']
    days_in_month = 31
    monthly_scale = days_in_month / config['days']
    
    scaled_volume = int(monthly_volume / monthly_scale)
    batch_size = 500000  # Increased from 200000 to 500000
    num_batches = math.ceil(scaled_volume / batch_size)
    
    # Calculate tracking counter start for this TPID
    tracking_counter = 100000001 + (tpid - 1000011) * 1000000
    
    # Process batches in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=24) as executor:
        batch_args = [
            (
                tracking_counter + (batch_num * batch_size),
                min(batch_size, scaled_volume - (batch_num * batch_size)),
                tpid,
                config['start_date'],
                config['end_date'],
                collection_name
            )
            for batch_num in range(num_batches)
        ]
        
        list(tqdm(
            executor.map(process_tpid_batch, batch_args),
            total=len(batch_args),
            desc=f"TPID {tpid} batches",
            unit="batch"
        ))

def process_tpid_wrapper(args):
    """Wrapper function to process a TPID with its collection"""
    tpid_info, collection_name, config = args
    return process_tpid(tpid_info, collection_name, config)

def process_collection(collection_name: str, config: Dict):
    try:
        logging.info(f"Setting up collection {collection_name}...")
        db = get_db_connection()
        collection = db[collection_name]
        
        # Drop collection if exists
        collection.drop()
        logging.info("Collection dropped")
        
        # Create time series collection
        logging.info("Creating time series collection...")
        db.create_collection(
            collection_name,
            timeseries={
                'timeField': 'timestamp',
                'metaField': 'tracking_reference',
                'granularity': 'minutes'
            }
        )
        logging.info("Time series collection created")
        
        # Create indexes
        logging.info("Creating indexes...")
        collection.create_index([("tpid", 1)])
        collection.create_index([("tracking_reference", 1)])
        collection.create_index([("edifact_code", 1)])
        collection.create_index([("timestamp", 1)])
        collection.create_index([("tpid", 1), ("timestamp", 1)])
        collection.create_index([("tpid", 1), ("edifact_code", 1)])
        logging.info("Indexes created")
        
        # Process fixed TPIDs in parallel
        logging.info(f"Processing fixed TPIDs for {collection_name}...")
        with concurrent.futures.ProcessPoolExecutor(max_workers=24) as executor:
            tpid_args = [(tpid_info, collection_name, config) for tpid_info in TPID_CONFIGS]
            list(tqdm(
                executor.map(process_tpid_wrapper, tpid_args),
                total=len(tpid_args),
                desc=f"{collection_name} - Fixed TPIDs",
                unit="tpid"
            ))
        
        # Process remaining TPIDs in parallel
        logging.info(f"Processing remaining TPIDs for {collection_name}...")
        total_monthly_parcels = config['total_parcels']
        days_in_month = 31
        monthly_scale = days_in_month / config['days']
        
        remaining_parcels = total_monthly_parcels - sum(
            int(volume / monthly_scale) for _, volume in TPID_CONFIGS
        )
        remaining_tpids = math.ceil(remaining_parcels / 50000)
        
        remaining_tpid_configs = [
            (tpid, 50000) for tpid in range(2000011, 2000011 + remaining_tpids)
        ]
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=24) as executor:
            remaining_args = [(tpid_info, collection_name, config) for tpid_info in remaining_tpid_configs]
            list(tqdm(
                executor.map(process_tpid_wrapper, remaining_args),
                total=len(remaining_args),
                desc=f"{collection_name} - Remaining TPIDs",
                unit="tpid"
            ))
        
        logging.info(f"Completed processing {collection_name}")
    
    except Exception as e:
        logging.error(f"Error processing collection {collection_name}: {str(e)}")
        raise

def main():
    try:
        logging.info("Starting database generation...")
        
        # Drop database if exists
        logging.info("Dropping existing database...")
        db = get_db_connection()
        db.client.drop_database('nzpost_summary_append')
        logging.info("Database dropped successfully")
        
        # Process collections sequentially
        for collection_name, config in COLLECTIONS.items():
            logging.info(f"Processing collection: {collection_name}")
            process_collection(collection_name, config)
            logging.info(f"Completed collection: {collection_name}")
            
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main() 