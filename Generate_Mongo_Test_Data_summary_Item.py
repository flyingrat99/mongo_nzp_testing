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
db = client['nzpost_summary_item']  # New database name

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

# TPID configurations with merchant mappings
TPID_CONFIGS = [
    (1000011, 500000, "OfficeMax New Zealand Ltd", "91804159"),
    (1000012, 300000, "Warehouse Stationery", "91804160"),
    (1000013, 200000, "Mighty Ape", "91804161"),
    (1000014, 100000, "PB Tech", "91804162"),
    (1000015, 75000, "Noel Leeming", "91804163"),
    (1000016, 50000, "Harvey Norman", "91804164"),
    (1000017, 25000, "JB Hi-Fi", "91804165"),
    (1000018, 10000, "Paper Plus", "91804166"),
    (1000019, 5000, "Whitcoulls", "91804167"),
    (1000020, 2000, "Farmers", "91804168")
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

# Product configurations
PRODUCT_CONFIGS = [
    {
        "service_code": "CPOLP",
        "description": "Courier Parcel",
        "service_standard": "OVERNIGHT",
        "sla_days": "1"
    },
    {
        "service_code": "CPOLR",
        "description": "Rural Parcel",
        "service_standard": "2-DAY",
        "sla_days": "2"
    },
    {
        "service_code": "CPOLS",
        "description": "Saturday Delivery",
        "service_standard": "SATURDAY",
        "sla_days": "1"
    }
]

# NZ Cities and Postcodes
NZ_LOCATIONS = [
    ("Auckland", ["2013", "2025", "2024", "2022", "2018"]),
    ("Wellington", ["6011", "6012", "6021", "6022", "6023"]),
    ("Christchurch", ["8011", "8013", "8014", "8022", "8024"]),
    ("Hamilton", ["3200", "3204", "3206", "3210", "3214"]),
    ("Tauranga", ["3110", "3112", "3116", "3118", "3120"])
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

def generate_address():
    city, postcodes = random.choice(NZ_LOCATIONS)
    street_number = random.randint(1, 2000)
    street_names = ["Queen St", "King St", "Victoria St", "Albert St", "High St", "Main St"]
    street = f"{street_number} {random.choice(street_names)}"
    return {
        "street": street,
        "suburb": f"{city} North",
        "city": city,
        "postcode": random.choice(postcodes),
        "country": "New Zealand",
        "address_id": str(random.randint(1000000, 9999999)),
        "dpid": str(random.randint(100000, 999999))
    }

def generate_parcel_details(tracking_ref: str, merchant_name: str) -> Dict:
    is_rural = random.random() < 0.2
    is_signature = random.random() < 0.3
    product = random.choice(PRODUCT_CONFIGS).copy()
    product.update({
        "is_signature_required": is_signature,
        "is_photo_required": random.random() < 0.4,
        "is_age_restricted": random.random() < 0.1,
        "is_rural": is_rural,
        "is_saturday": product["service_code"] == "CPOLS",
        "is_evening": random.random() < 0.15,
        "is_no_atl": random.random() < 0.05,
        "is_dangerous_goods": random.random() < 0.02,
        "is_xl": random.random() < 0.1,
        "initial_edd": (datetime.now() + timedelta(days=int(product["sla_days"]))).isoformat()
    })

    return {
        "base_tracking_reference": None,
        "carrier": "CourierPost",
        "is_return": random.random() < 0.05,
        "custom_item_id": str(random.randint(20000000, 29999999)),
        "merchant": {
            "name": merchant_name
        },
        "sender_details": {
            "company_name": merchant_name.upper(),
            "address": generate_address()
        },
        "Receiver_details": {
            "name": f"{random.choice(['John', 'Jane', 'James', 'Sarah', 'Michael'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones'])}",
            "phone": f"02{random.randint(10000000, 99999999)}",
            "address": generate_address()
        },
        "product": product,
        "value": {
            "amount": round(random.uniform(1.0, 500.0), 2),
            "currency": "NZD"
        },
        "weight_kg": round(random.uniform(0.1, 30.0), 2),
        "dimensions": {
            "length_mm": random.randint(100, 1000),
            "width_mm": random.randint(100, 1000),
            "height_mm": random.randint(100, 1000)
        }
    }

def generate_documents_batch(
    start_tracking: int,
    batch_size: int,
    tpid: int,
    merchant_name: str,
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
            "event_datetime": event_datetime,
            "parcel_details": generate_parcel_details(tracking_number, merchant_name)
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
        collection.create_index([("tracking_reference", 1)])
        collection.create_index([("parcel_details.custom_item_id", 1)])
        collection.create_index([("parcel_details.product.service_code", 1)])
        
        # Calculate TPID distribution
        total_monthly_parcels = config['total_parcels']
        days_in_month = 31  # March has 31 days
        monthly_scale = days_in_month / config['days']
        
        # Process fixed TPIDs
        tracking_counter = 100000001
        batch_size = 10000
        
        for tpid, monthly_volume, merchant_name, _ in TPID_CONFIGS:
            scaled_volume = int(monthly_volume / monthly_scale)
            num_batches = math.ceil(scaled_volume / batch_size)
            
            with tqdm(total=scaled_volume, desc=f"{collection_name} - TPID {tpid}") as pbar:
                for batch_num in range(num_batches):
                    current_batch_size = min(batch_size, scaled_volume - (batch_num * batch_size))
                    documents = generate_documents_batch(
                        tracking_counter,
                        current_batch_size,
                        tpid,
                        merchant_name,
                        config['start_date'],
                        config['end_date']
                    )
                    collection.insert_many(documents)
                    tracking_counter += current_batch_size
                    pbar.update(current_batch_size)
        
        # Process remaining TPIDs
        remaining_parcels = total_monthly_parcels - sum(
            int(volume / monthly_scale) for _, volume, _, _ in TPID_CONFIGS
        )
        remaining_tpids = math.ceil(remaining_parcels / 50000)
        
        for tpid in range(2000011, 2000011 + remaining_tpids):
            scaled_volume = 50000
            num_batches = math.ceil(scaled_volume / batch_size)
            merchant_name = f"Merchant {tpid}"
            
            with tqdm(total=scaled_volume, desc=f"{collection_name} - TPID {tpid}") as pbar:
                for batch_num in range(num_batches):
                    current_batch_size = min(batch_size, scaled_volume - (batch_num * batch_size))
                    documents = generate_documents_batch(
                        tracking_counter,
                        current_batch_size,
                        tpid,
                        merchant_name,
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
        client.drop_database('nzpost_summary_item')
        
        # Process collections sequentially
        for collection_name, config in COLLECTIONS.items():
            process_collection(collection_name, config)
            
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()

