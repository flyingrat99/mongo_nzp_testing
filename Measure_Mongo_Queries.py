import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
import pymongo
from pymongo import MongoClient
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import time
from typing import List, Dict, Optional
from PIL import Image, ImageTk
import json
from bson import ObjectId
import numpy as np

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')

# Edifact code mappings
EDIFACT_CODES = {
    "Picked Up": 100,
    "In Transit": 200,
    "In Depot": 300,
    "Out for Delivery": 400,
    "Delivered": 500,
    "Attempted Delivery": 600
}

# Collection configurations
COLLECTIONS = {
    "1 week (3M) - 1st - 7th March": "summary_1_week",
    "2 weeks (6M) - 1st - 14th March": "summary_2_weeks",
    "1 month (13M) - March": "summary_1_month",
    "3 months (38.6M) - Jan-Mar": "summary_3_months"
}

# Database display names
DATABASE_NAMES = {
    "nzpost_summary": "1 week (3M)",
    "nzpost_summary_item": "1 week with item details (3M)",
    "nzpost_summary_append": "1 week with time series (3M)"
}

# TPID monthly volumes
TPID_VOLUMES = {
    1000011: 500000,
    1000012: 300000,
    1000013: 200000,
    1000014: 100000,
    1000015: 75000,
    1000016: 50000,
    1000017: 25000,
    1000018: 10000,
    1000019: 5000,
    1000020: 2000
}

# Sample JSON for document structure
SAMPLE_JSON_SUMMARY = {
    "_id": ObjectId("67ef08ea7e382fec192d35d3"),
    "tracking_reference": "NZ100000001",
    "tpid": 1000011,
    "edifact_code": 500,
    "event_description": "Delivered",
    "event_datetime": datetime(2025, 3, 6, 4, 18, 28)
}

SAMPLE_JSON_ITEM = {
    "_id": ObjectId("67ef3b94f837b2dd6d381fc2"),
    "tracking_reference": "NZ100000001",
    "tpid": 1000011,
    "edifact_code": 400,
    "event_description": "Out for Delivery",
    "event_datetime": datetime(2025, 1, 22, 13, 59, 8),
    "parcel_details": {
        "base_tracking_reference": None,
        "carrier": "CourierPost",
        "is_return": False,
        "custom_item_id": "29226413",
        "merchant": {
            "name": "OfficeMax New Zealand Ltd"
        },
        "sender_details": {
            "company_name": "OFFICEMAX NEW ZEALAND LTD",
            "address": {
                "street": "1712 Albert St",
                "suburb": "Wellington North",
                "city": "Wellington",
                "postcode": "6011",
                "country": "New Zealand",
                "address_id": "3955920",
                "dpid": "821818"
            }
        },
        "Receiver_details": {
            "name": "John Johnson",
            "phone": "0228894938",
            "address": {
                "street": "1911 Queen St",
                "suburb": "Wellington North",
                "city": "Wellington",
                "postcode": "6022",
                "country": "New Zealand",
                "address_id": "8200735",
                "dpid": "929215"
            }
        },
        "product": {
            "service_code": "CPOLR",
            "description": "Rural Parcel",
            "service_standard": "2-DAY",
            "sla_days": "2",
            "is_signature_required": False,
            "is_photo_required": False,
            "is_age_restricted": False,
            "is_rural": False,
            "is_saturday": False,
            "is_evening": False,
            "is_no_atl": False,
            "is_dangerous_goods": False,
            "is_xl": False,
            "initial_edd": datetime(2025, 4, 6, 14, 53, 24, 575880)
        },
        "value": {
            "amount": 444.81,
            "currency": "NZD"
        },
        "weight_kg": 3.49,
        "dimensions": {
            "length_mm": 489,
            "width_mm": 708,
            "height_mm": 886
        }
    }
}

# Sample JSON for nzpost_summary_append database
SAMPLE_JSON_APPEND = {
    "_id": ObjectId("65f8a1b2c3d4e5f6a7b8c9d0"),
    "tracking_reference": "NZ100000001",
    "tpid": 1000011,
    "timestamp": datetime(2025, 3, 1, 10, 30, 0),  # First event timestamp
    "edifact_code": 100,
    "event_description": "Picked Up"
}

# Add index configurations after the SAMPLE_JSON constants
INDEX_INFO = {
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

class MongoQueryApp:
    def __init__(self, root):
        self.root = root
        self.root.title("MongoDB Query Performance Measurement")
        self.root.geometry("1200x1200")  # Increased height to 1200
        
        # Initialize database connection
        self.client = MongoClient('mongodb://localhost:27017/')
        self.current_db = "nzpost_summary"
        self.db = self.client[self.current_db]
        
        # Header with Team Vulcan logo
        self.header_frame = ttk.Frame(root)
        self.header_frame.pack(fill=tk.X, padx=10, pady=5)
        
        try:
            # Load and resize the logo
            logo_img = Image.open("vulcan_logo.png")
            # Resize to reasonable height while maintaining aspect ratio
            logo_height = 60
            aspect_ratio = logo_img.width / logo_img.height
            logo_width = int(logo_height * aspect_ratio)
            logo_img = logo_img.resize((logo_width, logo_height), Image.Resampling.LANCZOS)
            logo_photo = ImageTk.PhotoImage(logo_img)
            
            # Create and pack the logo label
            logo_label = ttk.Label(self.header_frame, image=logo_photo)
            logo_label.image = logo_photo  # Keep a reference to prevent garbage collection
            logo_label.pack(side=tk.LEFT, padx=5)
        except Exception as e:
            print(f"Error loading logo: {e}")
        
        # Team Vulcan text
        team_label = ttk.Label(self.header_frame, text="Team Vulcan",
                             font=("Arial", 24, "bold"))
        team_label.pack(side=tk.LEFT, padx=20)
        
        # Add separator
        ttk.Separator(root, orient='horizontal').pack(fill=tk.X, padx=5, pady=5)
        
        # Connection status
        self.connection_frame = ttk.LabelFrame(root, text="Connection Status", padding=10)
        self.connection_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Database selection
        self.db_frame = ttk.Frame(self.connection_frame)
        self.db_frame.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(self.db_frame, text="Database:").pack(side=tk.LEFT)
        self.db_var = tk.StringVar(value="nzpost_summary")
        self.db_dropdown = ttk.Combobox(self.db_frame, textvariable=self.db_var, 
                                      values=["nzpost_summary", "nzpost_summary_item", "nzpost_summary_append"],
                                      state="readonly", width=20)
        self.db_dropdown.pack(side=tk.LEFT, padx=5)
        self.db_dropdown.bind("<<ComboboxSelected>>", self.on_database_change)
        
        self.status_label = ttk.Label(self.connection_frame, text="Connected", foreground="green")
        self.status_label.pack(side=tk.LEFT, padx=10)
        
        # Add Sample JSON button
        self.sample_json_btn = ttk.Button(self.connection_frame, text="Sample JSON",
                                        command=self.show_sample_json)
        self.sample_json_btn.pack(side=tk.LEFT, padx=10)
        
        # Add Indexes button next to Sample JSON button
        self.indexes_btn = ttk.Button(self.connection_frame, text="Indexes",
                                    command=self.show_indexes)
        self.indexes_btn.pack(side=tk.LEFT, padx=10)
        
        # Create a container frame for Data Range and Date Range
        self.range_container = ttk.Frame(root)
        self.range_container.pack(fill=tk.X, padx=10, pady=5)
        
        # Data Range Selection with radio buttons
        self.range_frame = ttk.LabelFrame(self.range_container, text="Data Range", padding=10)
        self.range_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        
        self.collection_var = tk.StringVar(value="1 week (3M) - 1st - 7th March")
        for text, _ in COLLECTIONS.items():
            ttk.Radiobutton(self.range_frame, text=text, variable=self.collection_var, value=text,
                           command=self.update_date_range).pack(anchor=tk.W)
        
        # Date Range Selection with calendar widgets
        self.date_frame = ttk.LabelFrame(self.range_container, text="Date Range", padding=10)
        self.date_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(5, 0))
        
        self.use_date_range = tk.BooleanVar()
        self.date_range_checkbox = ttk.Checkbutton(self.date_frame, text="Use Date Range", 
                              variable=self.use_date_range, 
                              command=self.toggle_date_pickers)
        self.date_range_checkbox.pack(anchor=tk.W)
        
        # Initialize with default dates (March 1-7, 2025)
        default_from_date = datetime(2025, 3, 1)
        default_to_date = datetime(2025, 3, 7)
        
        date_controls = ttk.Frame(self.date_frame)
        date_controls.pack(fill=tk.X, pady=(5, 0))
        
        ttk.Label(date_controls, text="From:").pack(side=tk.LEFT, padx=5)
        self.from_date = DateEntry(date_controls, width=12, background='darkblue',
                                 foreground='white', borderwidth=2, date_pattern='dd/mm/yy',
                                 year=2025, month=3, day=1)
        self.from_date.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(date_controls, text="To:").pack(side=tk.LEFT, padx=5)
        self.to_date = DateEntry(date_controls, width=12, background='darkblue',
                               foreground='white', borderwidth=2, date_pattern='dd/mm/yy',
                               year=2025, month=3, day=7)
        self.to_date.pack(side=tk.LEFT, padx=5)
        
        # TPID Selection
        self.tpid_frame = ttk.LabelFrame(root, text="TPID Selection", padding=10)
        self.tpid_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.tpid_vars = {}
        self.tpid_checkbuttons = {}  # Store references to checkbuttons
        for i in range(10):
            tpid = 1000011 + i
            var = tk.BooleanVar(value=(tpid == 1000011))  # Set 1000011 to be checked by default
            self.tpid_vars[tpid] = var
            
            # Create checkbutton
            cb = ttk.Checkbutton(self.tpid_frame, text=f"TPID {tpid}", variable=var)
            cb.pack(side=tk.LEFT, padx=5)
            self.tpid_checkbuttons[tpid] = cb
            
            # Bind hover events
            cb.bind("<Enter>", lambda e, t=tpid: self.show_tpid_volume(e, t))
            cb.bind("<Leave>", self.hide_tooltip)
        
        # Status Buttons
        self.status_frame = ttk.Frame(root, padding=10)
        self.status_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.status_buttons = {}
        self.active_button = None  # Track the currently active button
        
        # Create styles for the buttons
        style = ttk.Style()
        style.configure("Status.TButton", padding=5)
        style.configure("Active.TButton", padding=5, background="#0078D7")
        style.map("Active.TButton",
                 background=[("active", "#0078D7"), ("pressed", "#0078D7")],
                 foreground=[("active", "white"), ("pressed", "white")])
        
        for status, code in EDIFACT_CODES.items():
            btn = ttk.Button(self.status_frame, text=status, style="Status.TButton",
                           command=lambda s=status: self.handle_status_click(s))
            btn.pack(side=tk.LEFT, padx=5)
            self.status_buttons[status] = btn
            
            # Set up tooltip for status buttons
            btn.bind("<Enter>", lambda e, s=status: self.show_query_details(e, s))
            btn.bind("<Leave>", self.hide_query_details)
        
        # Results Display
        self.results_frame = ttk.LabelFrame(root, text="Query Results", padding=10)
        self.results_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Left-aligned results with query details
        self.results_container = ttk.Frame(self.results_frame)
        self.results_container.pack(fill=tk.X, expand=True)
        
        # Results on the left
        self.results_left = ttk.Frame(self.results_container)
        self.results_left.pack(side=tk.LEFT, padx=20)
        
        self.count_label = ttk.Label(self.results_left, text="Count: 0", 
                                   font=("Arial", 24, "bold"), foreground="red",
                                   justify=tk.LEFT)
        self.count_label.pack(anchor=tk.W, pady=5)
        
        self.time_label = ttk.Label(self.results_left, text="Response Time: 0ms",
                                  justify=tk.LEFT)
        self.time_label.pack(anchor=tk.W, pady=2)
        
        self.parcels_label = ttk.Label(self.results_left, text="Parcels: 0",
                                     justify=tk.LEFT)
        self.parcels_label.pack(anchor=tk.W, pady=2)
        
        # Query details on the right
        self.results_right = ttk.Frame(self.results_container)
        self.results_right.pack(side=tk.LEFT, padx=20, fill=tk.X, expand=True)
        
        self.query_label = ttk.Label(self.results_right, text="Aggregation Pipeline Query:",
                                   justify=tk.LEFT, wraplength=600)
        self.query_label.pack(anchor=tk.W)
        
        # Query Button Frame
        self.query_frame = ttk.Frame(root, padding=10)
        self.query_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Left side: Performance Test Button with tooltip
        self.button_frame = ttk.Frame(self.query_frame)
        self.button_frame.pack(side=tk.LEFT, padx=5)
        
        self.performance_btn = ttk.Button(self.button_frame, text="Performance Test", 
                                        command=self.run_performance_test)
        self.performance_btn.pack(side=tk.LEFT)
        
        # Add Pipeline Details button next to Performance Test
        self.perf_pipeline_btn = ttk.Button(self.button_frame, text="Show Pipeline Details",
                                          command=self.show_performance_pipeline)
        self.perf_pipeline_btn.pack(side=tk.LEFT, padx=5)
        
        self.performance_btn.bind("<Enter>", self.show_performance_details)
        self.performance_btn.bind("<Leave>", self.hide_query_details)
        
        # Right side: Performance Times Display
        self.perf_times_frame = ttk.Frame(self.query_frame)
        self.perf_times_frame.pack(side=tk.LEFT, padx=20, fill=tk.X, expand=True)
        
        # Create labels for each database's performance time
        self.perf_time_labels = {}
        for collection_display, collection_name in COLLECTIONS.items():
            label = ttk.Label(self.perf_times_frame, 
                            text=f"{collection_display}: Not tested",
                            justify=tk.LEFT)
            label.pack(anchor=tk.W)
            self.perf_time_labels[collection_name] = label
        
        # Graph Frame
        self.graph_frame = ttk.Frame(root)
        self.graph_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Initialize date pickers state
        self.toggle_date_pickers()
        
        # Set initial date range based on default collection
        self.update_date_range()
        
        # Track selected statuses
        self.selected_statuses = set()
        
        # Close window handler
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def toggle_date_pickers(self):
        state = "normal" if self.use_date_range.get() else "disabled"
        self.from_date.configure(state=state)
        self.to_date.configure(state=state)
        
        # Force update of the displayed dates
        collection_name = self.collection_var.get()
        self.update_date_range()
    
    def toggle_status(self, status):
        if status in self.selected_statuses:
            self.selected_statuses.remove(status)
            self.status_buttons[status].state(['!pressed'])
        else:
            self.selected_statuses.add(status)
            self.status_buttons[status].state(['pressed'])
    
    def hide_tooltip(self, event=None):
        """Hide the current tooltip if it exists."""
        try:
            # Cancel any pending tooltip destruction if it exists
            if hasattr(self, 'tooltip_after_id') and self.tooltip_after_id:
                try:
                    self.root.after_cancel(self.tooltip_after_id)
                except ValueError:
                    pass  # Timer might not exist anymore
                self.tooltip_after_id = None
            
            # Destroy tooltips if they exist
            if hasattr(self, 'current_tooltip') and self.current_tooltip:
                try:
                    self.current_tooltip.destroy()
                except:
                    pass  # Window might already be destroyed
                self.current_tooltip = None
                
            if hasattr(self, 'tooltip') and self.tooltip:
                try:
                    self.tooltip.destroy()
                except:
                    pass  # Window might already be destroyed
                self.tooltip = None
                
        except Exception as e:
            print(f"Error in hide_tooltip: {str(e)}")

    def hide_query_details(self, event=None):
        """Hide query details tooltip."""
        self.hide_tooltip()  # Reuse the main hide_tooltip method

    def show_query_details(self, event, status=None):
        """Show query details in a tooltip"""
        try:
            # First clean up any existing tooltips
            self.hide_tooltip()
            
            details = self.get_query_details(status)
            
            # Create tooltip window
            tooltip = tk.Toplevel()
            tooltip.wm_overrideredirect(True)
            tooltip.wm_geometry(f"+{event.x_root + 10}+{event.y_root + 10}")
            
            # Add label with details
            label = ttk.Label(tooltip, text=details, justify=tk.LEFT,
                             background="#ffffe0", relief=tk.SOLID, borderwidth=1)
            label.pack()
            
            # Store tooltip reference
            self.current_tooltip = tooltip
            
            # Schedule tooltip destruction
            self.tooltip_after_id = self.root.after(5000, lambda: self.hide_tooltip())
            
        except Exception as e:
            print(f"Error showing query details: {str(e)}")
            self.hide_tooltip()
    
    def get_query_details(self, status=None) -> str:
        details = []
        details.append(f"Collection: {self.collection_var.get()}")
        
        selected_tpids = [tpid for tpid, var in self.tpid_vars.items() if var.get()]
        if selected_tpids:
            details.append(f"\nSelected TPIDs: {', '.join(map(str, selected_tpids))}")
        else:
            details.append("\nNo TPIDs selected")
        
        if status:
            details.append(f"\nStatus Filter: {status}")
            details.append(f"Edifact Code: {EDIFACT_CODES[status]}")
        
        if self.use_date_range.get():
            details.append(f"\nDate Range:")
            details.append(f"From: {self.from_date.get_date()}")
            details.append(f"To: {self.to_date.get_date()}")
        
        # Build the query to show
        match_stage = self.build_query(status)
        pipeline = [
            {"$match": match_stage},
            {"$count": "total"}
        ]
        
        # Get the appropriate index hint
        hint = self.get_optimal_hint(match_stage)
        
        details.append(f"\nPipeline:")
        details.append(json.dumps(pipeline, indent=2, default=str))
        details.append(f"\nUsing Index Hint: {hint}")
        
        return "\n".join(details)
    
    def build_query(self, status=None) -> Dict:
        query = {}
        current_db = self.db_var.get()
        
        # Add TPID filter
        selected_tpids = [tpid for tpid, var in self.tpid_vars.items() if var.get()]
        if selected_tpids:
            query["tpid"] = {"$in": selected_tpids}
        
        # Add Edifact Code filter
        if status:
            query["edifact_code"] = EDIFACT_CODES[status]
        
        # Add Date Range filter if enabled
        if self.use_date_range.get():
            from_date = datetime.combine(self.from_date.get_date(), datetime.min.time())
            to_date = datetime.combine(self.to_date.get_date(), datetime.max.time())
            
            if current_db == "nzpost_summary_append":
                # For time series database, ensure timestamp is used
                query["timestamp"] = {"$gte": from_date, "$lte": to_date}
                
                # Print detailed debug info for time series queries
                print(f"\n=== Time Series Query Building ===")
                print(f"Date Range: {from_date} to {to_date}")
                print(f"Using timestamp field for time series query")
            else:
                query["event_datetime"] = {"$gte": from_date, "$lte": to_date}
        
        # Debug output
        if current_db == "nzpost_summary_append":
            print(f"Final query for time series: {json.dumps(query, default=str)}")
        
        return query
    
    def run_optimized_time_series_query(self, collection, pipeline, hint, timeout_ms):
        """Run an optimized query for time series collections with better performance"""
        print("\n=== Running Optimized Time Series Query ===")
        
        # Start timing
        start_time = time.time()
        
        # For time series collections, optimize the query by:
        # 1. Setting allowDiskUse to True
        # 2. Using a smaller batch size for better memory usage
        # 3. Using maxTimeMS to prevent long-running queries
        cursor = collection.aggregate(
            pipeline,
            allowDiskUse=True,
            hint=hint,
            batchSize=1000,  # Use smaller batch size
            maxTimeMS=timeout_ms  # Use the specified timeout
        )
        
        # Process the results immediately to avoid lazy evaluation delays
        result = list(cursor)
        
        # End timing
        end_time = time.time()
        
        # Calculate response time
        response_time = (end_time - start_time) * 1000
        print(f"Optimized query execution time: {response_time:.2f}ms")
        
        return result, response_time

    def run_status_query(self, status):
        """Run a query for the selected status and update the display"""
        try:
            # Get the actual collection name from the display name
            collection_name = COLLECTIONS[self.collection_var.get()]
            collection = self.db[collection_name]  # Use the actual collection name
            match_stage = self.build_query(status)
            
            # Get selected TPIDs for query
            selected_tpids = [tpid for tpid, var in self.tpid_vars.items() if var.get()]
            tpid_query = {"tpid": {"$in": selected_tpids}} if selected_tpids else {}
            
            # Print query details to console
            print("\n=== Query Details ===")
            print(f"Database: {self.current_db}")
            print(f"Collection: {collection_name}")
            print(f"Status: {status}")
            print(f"Selected TPIDs: {selected_tpids}")
            if self.use_date_range.get():
                print(f"Date Range: {self.from_date.get_date()} to {self.to_date.get_date()}")
            
            # Detailed performance tracking
            perf_stages = {}
            
            # Pipeline for total parcels count
            pipeline_total = [
                {"$match": tpid_query},
                {"$count": "total"}
            ]
            
            # Execute total parcels query with appropriate index hint
            print("\n=== Total Parcels Query ===")
            print(f"Pipeline: {json.dumps(pipeline_total, indent=2, default=str)}")
            hint_for_total = "tpid_1" if selected_tpids else None
            print(f"Index Hint: {hint_for_total}")
            
            perf_stages["total_parcels_start"] = time.time()
            
            # Use optimized query for time series collections
            if self.current_db == "nzpost_summary_append":
                total_result, total_time = self.run_optimized_time_series_query(
                    collection, pipeline_total, hint_for_total, 30000
                )
            else:
                # Standard query execution for regular collections
                total_result = list(collection.aggregate(
                    pipeline_total,
                    allowDiskUse=True,
                    hint=hint_for_total
                ))
            
            perf_stages["total_parcels_end"] = time.time()
            
            total_parcels = total_result[0]["total"] if total_result else 0
            self.parcels_label.config(text=f"Parcels: {total_parcels:,}")
            
            # Build main query pipeline based on database type
            if self.current_db == "nzpost_summary_append":
                # For time series, build a pipeline to find parcels with LATEST edifact_code matching the status
                
                # Extract date range from match_stage if present
                date_filter = {}
                if "timestamp" in match_stage:
                    date_filter = {"timestamp": match_stage["timestamp"]}
                
                # First filter for TPIDs if selected
                tpid_filter = {}
                if "tpid" in match_stage:
                    tpid_filter = {"tpid": match_stage["tpid"]}
                
                # Build pipeline for latest event
                pipeline = [
                    # Match by TPIDs and date range first if specified
                    {"$match": {**tpid_filter, **date_filter}},
                    # Sort by tracking reference and timestamp (descending to get latest first)
                    {"$sort": {"tracking_reference": 1, "timestamp": -1}},
                    # Group by tracking reference and take the first (latest) document
                    {"$group": {
                        "_id": "$tracking_reference",
                        "latest_code": {"$first": "$edifact_code"}
                    }},
                    # Match only those with the specified edifact code
                    {"$match": {"latest_code": EDIFACT_CODES[status]}},
                    # Count total
                    {"$count": "total"}
                ]
            else:
                # Standard query with simple match and count
                pipeline = [
                    {"$match": match_stage},
                    {"$count": "total"}
                ]
            
            # Determine optimal index based on query conditions
            hint = self.get_optimal_hint(match_stage)
            
            # Print main query details
            print("\n=== Main Query ===")
            print(f"Pipeline: {json.dumps(pipeline, indent=2, default=str)}")
            print(f"Index Hint: {hint}")
            print(f"Pipeline will be run on server side: {collection.database.client.address}")
            
            # Execute query and measure performance
            perf_stages["main_query_start"] = time.time()
            print(f"Query starting at: {datetime.now().strftime('%H:%M:%S.%f')}")
            
            # Run optimized query for time series collections
            if self.current_db == "nzpost_summary_append":
                result, response_time = self.run_optimized_time_series_query(
                    collection, pipeline, hint, 30000
                )
                count = result[0]["total"] if result else 0
            else:
                # Standard query execution for regular collections
                start_time = time.time()
                result = list(collection.aggregate(
                    pipeline,
                    allowDiskUse=True,
                    hint=hint
                ))
                end_time = time.time()
                count = result[0]["total"] if result else 0
                response_time = (end_time - start_time) * 1000  # Convert to milliseconds
            
            perf_stages["main_query_end"] = time.time()
            print(f"Query completed at: {datetime.now().strftime('%H:%M:%S.%f')}")
            
            # Print query results and performance breakdown
            print("\n=== Query Results ===")
            print(f"Count: {count:,}")
            print(f"Response Time: {response_time:.2f}ms")
            
            # Performance breakdown
            print("\n=== Performance Breakdown ===")
            total_parcels_time = (perf_stages["total_parcels_end"] - perf_stages["total_parcels_start"]) * 1000
            main_query_time = (perf_stages["main_query_end"] - perf_stages["main_query_start"]) * 1000
            print(f"Total Parcels Query: {total_parcels_time:.2f}ms")
            print(f"Main Query: {main_query_time:.2f}ms")
            print(f"Aggregation Execution: {response_time:.2f}ms")
            print("=" * 50 + "\n")
            
            self.count_label.config(text=f"Count: {count:,}")
            self.time_label.config(text=f"Response Time: {response_time:.2f}ms")
            
            # Remove old pipeline button if it exists
            if hasattr(self, 'pipeline_btn'):
                self.pipeline_btn.destroy()
            
            # Create new pipeline button
            self.pipeline_btn = ttk.Button(
                self.results_right,
                text="Show Pipeline Details",
                command=self.show_pipeline
            )
            self.pipeline_btn.pack(anchor=tk.W, pady=5)
            
        except Exception as e:
            error_msg = f"Error executing query: {str(e)}"
            print(f"\n=== Query Error ===\n{error_msg}\n")
            messagebox.showerror("Query Error", error_msg)
            self.count_label.config(text="Count: Error")
            self.time_label.config(text="Response Time: Error")
    
    def run_performance_test(self):
        test_tpids = [1000011, 1000012, 1000013, 1000014, 1000015]
        test_codes = [500, 600]  # Delivered and Attempted Delivery
        
        results = {}
        current_db = self.db_var.get()
        db = self.client[current_db]
        
        print(f"\n=== Running Performance Test for {current_db} ===")
        print(f"Testing all collections with TPIDs: {test_tpids}")
        print(f"Testing status codes: {test_codes}")
        
        # Date ranges for collections
        collection_date_ranges = {
            "summary_1_week": (datetime(2025, 3, 1), datetime(2025, 3, 7)),
            "summary_2_weeks": (datetime(2025, 3, 1), datetime(2025, 3, 14)),
            "summary_1_month": (datetime(2025, 3, 1), datetime(2025, 3, 31)),
            "summary_3_months": (datetime(2025, 1, 1), datetime(2025, 3, 31))
        }
        
        # Test each collection in the current database
        for display_name, collection_name in COLLECTIONS.items():
            print(f"\n--- Testing Collection: {collection_name} ({display_name}) ---")
            collection = db[collection_name]
            
            match_stage = {
                "tpid": {"$in": test_tpids},
                "edifact_code": {"$in": test_codes}
            }
            
            # Always add date range for time series collections, or if enabled for regular collections
            if current_db == "nzpost_summary_append" or self.use_date_range.get():
                from_date, to_date = collection_date_ranges[collection_name]
                from_date = datetime.combine(from_date, datetime.min.time())
                to_date = datetime.combine(to_date, datetime.max.time())
                
                if current_db == "nzpost_summary_append":
                    match_stage["timestamp"] = {
                        "$gte": from_date,
                        "$lte": to_date
                    }
                else:
                    match_stage["event_datetime"] = {
                        "$gte": from_date,
                        "$lte": to_date
                    }
                
                print(f"Using date range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
            
            # Determine which index to use based on the query
            hint = None
            if current_db == "nzpost_summary_append":
                if "timestamp" in match_stage:
                    hint = "tpid_1_timestamp_1"
                else:
                    hint = "tpid_1_edifact_code_1"
            else:
                if "event_datetime" in match_stage:
                    hint = "tpid_1_edifact_code_1_event_datetime_1"
                else:
                    hint = "tpid_1_edifact_code_1"
            
            try:
                print(f"Running query with hint: {hint}")
                
                # For time series collections, count only parcels with latest edifact code matching criteria
                if current_db == "nzpost_summary_append":
                    # Calculate timeout based on collection size
                    if collection_name == "summary_3_months":
                        timeout_ms = 120000  # 120 seconds for 3 months collection
                    elif collection_name == "summary_1_month":
                        timeout_ms = 60000   # 60 seconds for 1 month collection
                    elif collection_name == "summary_2_weeks":
                        timeout_ms = 45000   # 45 seconds for 2 weeks collection
                    else:
                        timeout_ms = 30000   # 30 seconds for 1 week collection
                    
                    # Pipeline to get latest event for each tracking reference where TPID matches
                    latest_parcels_pipeline = [
                        # Match TPIDs first to limit dataset
                        {"$match": {"tpid": {"$in": test_tpids}, "timestamp": {"$gte": from_date, "$lte": to_date}}},
                        # Sort by tracking reference and timestamp (descending to get latest first)
                        {"$sort": {"tracking_reference": 1, "timestamp": -1}},
                        # Group by tracking reference and take the first (latest) document
                        {"$group": {
                            "_id": "$tracking_reference",
                            "latest_code": {"$first": "$edifact_code"}
                        }},
                        # Match only those with the specified edifact codes
                        {"$match": {"latest_code": {"$in": test_codes}}},
                        # Count total
                        {"$count": "total"}
                    ]
                    
                    # Run the query to count parcels with latest event matching criteria
                    parcels_result, response_time = self.run_optimized_time_series_query(
                        collection, latest_parcels_pipeline, hint, timeout_ms
                    )
                    parcels_count = parcels_result[0]["total"] if parcels_result else 0
                    
                    print(f"Query completed in {response_time:.2f}ms, found {parcels_count:,} parcels with latest edifact_code = 500 or 600")
                    
                    results[display_name] = {
                        "time": response_time,
                        "count": parcels_count
                    }
                    
                    # Update the performance time label
                    self.perf_time_labels[collection_name].config(
                        text=f"{display_name}: {response_time:.2f}ms ({parcels_count:,} parcels)"
                    )
                else:
                    # Standard query execution for regular collections
                    # Optimize the pipeline for counting
                    pipeline = [
                        {"$match": match_stage},
                        {"$count": "total"}
                    ]
                    
                    start_time = time.time()
                    result = list(collection.aggregate(
                        pipeline,
                        allowDiskUse=True,
                        hint=hint
                    ))
                    end_time = time.time()
                    count = result[0]["total"] if result else 0
                    response_time = (end_time - start_time) * 1000  # Convert to milliseconds
                
                    print(f"Query completed in {response_time:.2f}ms, found {count:,} documents")
                    
                    results[display_name] = {
                        "time": response_time,
                        "count": count
                    }
                    
                    # Update the performance time label
                    self.perf_time_labels[collection_name].config(
                        text=f"{display_name}: {response_time:.2f}ms ({count:,} documents)"
                    )
                
            except Exception as e:
                error_msg = f"Error in {collection_name}: {str(e)}"
                print(f"ERROR: {error_msg}")
                messagebox.showerror("Query Error", error_msg)
                results[display_name] = {
                    "time": 0,
                    "count": 0
                }
                self.perf_time_labels[collection_name].config(
                    text=f"{display_name}: Error"
                )
        
        # Create bar graph
        self.plot_results(results)
    
    def plot_results(self, results: Dict):
        # Clear previous graph
        for widget in self.graph_frame.winfo_children():
            widget.destroy()
        
        # Create a single graph for all databases
        fig, ax = plt.subplots(figsize=(12, 10))
        collections = list(results.keys())
        times = [results[col]["time"] for col in collections]
        counts = [results[col]["count"] for col in collections]
        
        bars = ax.bar(collections, times, color='blue')
        ax.set_ylabel('Response Time (ms)')
        
        # Set title based on current database
        current_db = self.db_var.get()
        if current_db == "nzpost_summary_append":
            ax.set_title('Query Performance - Unique Parcels with Latest Status = Delivered/Attempted')
        else:
            ax.set_title('Query Performance Across Collections')
        
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=15, ha='right')
        
        # Add count labels above bars
        for bar, count in zip(bars, counts):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 5,
                   f'Count: {count:,}',
                   ha='center', va='bottom')
        
        # Add more padding to prevent label cutoff
        plt.tight_layout()
        
        canvas = FigureCanvasTkAgg(fig, master=self.graph_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    
    def on_closing(self):
        plt.close('all')  # Close all matplotlib figures
        self.root.quit()  # Stop the mainloop
        self.root.destroy()  # Destroy the window
        exit(0)  # Exit the application

    def update_date_range(self):
        """Update the date range based on the selected collection"""
        collection_name = self.collection_var.get()
        
        # Store current state and temporarily enable if disabled
        current_state = self.from_date.cget("state")
        if current_state == "disabled":
            self.from_date.configure(state="normal")
            self.to_date.configure(state="normal")
        
        # Update the date range
        if collection_name == "1 week (3M) - 1st - 7th March":
            self.from_date.set_date(datetime(2025, 3, 1))
            self.to_date.set_date(datetime(2025, 3, 7))
        elif collection_name == "2 weeks (6M) - 1st - 14th March":
            self.from_date.set_date(datetime(2025, 3, 1))
            self.to_date.set_date(datetime(2025, 3, 14))
        elif collection_name == "1 month (13M) - March":
            self.from_date.set_date(datetime(2025, 3, 1))
            self.to_date.set_date(datetime(2025, 3, 31))
        elif collection_name == "3 months (38.6M) - Jan-Mar":
            self.from_date.set_date(datetime(2025, 1, 1))
            self.to_date.set_date(datetime(2025, 3, 31))
        
        # Restore original state if it was disabled
        if current_state == "disabled":
            self.from_date.configure(state="disabled")
            self.to_date.configure(state="disabled")

    def handle_status_click(self, status):
        """Handle status button click - update button styles and run query"""
        # Reset previous active button style
        if self.active_button:
            self.active_button.configure(style="Status.TButton")
        
        # Set new active button style
        current_button = self.status_buttons[status]
        current_button.configure(style="Active.TButton")
        self.active_button = current_button
        
        # Run the query
        self.run_status_query(status)

    def show_performance_details(self, event):
        """Show performance test query details in tooltip"""
        try:
            # First clean up any existing tooltips
            self.hide_tooltip()
            
            details = []
            details.append("Performance Test Query Details:")
            details.append("TPIDs: 1000011, 1000012, 1000013, 1000014, 1000015")
            details.append("Edifact Codes: 500 - Delivered, 600 - Attempted Delivery")
            
            if self.use_date_range.get():
                from_date = self.from_date.get_date()
                to_date = self.to_date.get_date()
                details.append(f"Date Range: {from_date} to {to_date}")
            
            details.append(f"\nWill test all collections in {self.db_var.get()}:")
            for collection_display in COLLECTIONS.keys():
                details.append(f"- {collection_display}")
            
            tooltip = tk.Toplevel(self.root)
            tooltip.wm_overrideredirect(True)
            tooltip.wm_geometry(f"+{event.x_root+10}+{event.y_root+10}")
            label = ttk.Label(tooltip, text="\n".join(details), justify=tk.LEFT,
                             background="#ffffe0", relief='solid', borderwidth=1)
            label.pack()
            
            self.tooltip = tooltip
            
            # Schedule tooltip destruction
            self.tooltip_after_id = self.root.after(5000, lambda: self.hide_tooltip())
            
        except Exception as e:
            print(f"Error showing performance details: {str(e)}")
            self.hide_tooltip()

    def show_tpid_volume(self, event, tpid):
        """Show tooltip with TPID volume information."""
        try:
            # First clean up any existing tooltips
            self.hide_tooltip()
            
            tooltip = tk.Toplevel(self.root)
            tooltip.wm_overrideredirect(True)
            tooltip.wm_geometry(f"+{event.x_root+10}+{event.y_root+10}")
            
            volume = TPID_VOLUMES[tpid]
            volume_text = f"{volume:,} parcels/month"
            
            label = ttk.Label(tooltip, text=volume_text, justify=tk.LEFT,
                             background="#ffffe0", relief='solid', borderwidth=1,
                             padding=5)
            label.pack()
            
            self.current_tooltip = tooltip
            
            # Schedule tooltip destruction
            self.tooltip_after_id = self.root.after(5000, lambda: self.hide_tooltip())
            
        except Exception as e:
            print(f"Error showing TPID volume: {str(e)}")
            self.hide_tooltip()

    def show_sample_json(self):
        """Show sample JSON structure in a popup window"""
        # Create a new window
        popup = tk.Toplevel(self.root)
        popup.title("Sample JSON Structure")
        popup.geometry("800x600")  # Increased window size
        
        # Create text widget with scrollbar
        text_frame = ttk.Frame(popup)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        text = tk.Text(text_frame, wrap=tk.WORD, yscrollcommand=scrollbar.set)
        text.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=text.yview)
        
        # Insert appropriate sample JSON based on selected database
        if self.db_var.get() == "nzpost_summary":
            sample_json = SAMPLE_JSON_SUMMARY
        elif self.db_var.get() == "nzpost_summary_item":
            sample_json = SAMPLE_JSON_ITEM
        else:  # nzpost_summary_append
            sample_json = SAMPLE_JSON_APPEND
        
        # Format and insert the JSON
        formatted_json = json.dumps(sample_json, indent=2, default=str)
        text.insert(tk.END, formatted_json)
        
        # Add explanation for append database
        if self.db_var.get() == "nzpost_summary_append":
            text.insert(tk.END, "\n\nNote: In the append database, each event is stored as a separate document.\n")
            text.insert(tk.END, "A single parcel's journey would be represented by multiple documents like this:\n\n")
            
            # Example of multiple events for the same parcel
            events = [
                {
                    "_id": ObjectId("65f8a1b2c3d4e5f6a7b8c9d1"),
                    "tracking_reference": "NZ100000001",
                    "tpid": 1000011,
                    "timestamp": datetime(2025, 3, 1, 12, 45, 0),
                    "edifact_code": 200,
                    "event_description": "In Transit"
                },
                {
                    "_id": ObjectId("65f8a1b2c3d4e5f6a7b8c9d2"),
                    "tracking_reference": "NZ100000001",
                    "tpid": 1000011,
                    "timestamp": datetime(2025, 3, 1, 15, 20, 0),
                    "edifact_code": 300,
                    "event_description": "In Depot"
                },
                {
                    "_id": ObjectId("65f8a1b2c3d4e5f6a7b8c9d3"),
                    "tracking_reference": "NZ100000001",
                    "tpid": 1000011,
                    "timestamp": datetime(2025, 3, 2, 9, 30, 0),
                    "edifact_code": 400,
                    "event_description": "Out for Delivery"
                },
                {
                    "_id": ObjectId("65f8a1b2c3d4e5f6a7b8c9d4"),
                    "tracking_reference": "NZ100000001",
                    "tpid": 1000011,
                    "timestamp": datetime(2025, 3, 2, 11, 15, 0),
                    "edifact_code": 500,
                    "event_description": "Delivered"
                }
            ]
            
            for event in events:
                text.insert(tk.END, "\n" + json.dumps(event, indent=2, default=str))
        
        # Make text read-only
        text.config(state=tk.DISABLED)
        
        # Add close button
        close_button = ttk.Button(popup, text="Close", command=popup.destroy)
        close_button.pack(pady=10)

    def on_database_change(self, event):
        """Handle database selection change"""
        # Store old database for cleanup
        old_db = self.current_db
        
        # Get the new database value
        self.current_db = self.db_var.get()
        print(f"Database changed to: {self.current_db}")
        
        # Connect to the new database
        self.db = self.client[self.current_db]
        
        # Update the query if there's an active status
        if self.active_button:
            self.run_status_query(self.active_button.cget("text"))
            
        # Enforce date range for time series database
        if self.current_db == "nzpost_summary_append":
            # Enable date range and disable the checkbox
            self.use_date_range.set(True)
            self.date_range_checkbox.config(state=tk.DISABLED)
            self.from_date.config(state="normal")
            self.to_date.config(state="normal")
        else:
            # For other databases, make the checkbox enabled
            self.date_range_checkbox.config(state=tk.NORMAL)
            # Update date inputs based on checkbox state
            self.toggle_date_pickers()

    def show_indexes(self):
        """Show index information for each database in a popup window"""
        # Create a new window
        popup = tk.Toplevel(self.root)
        popup.title("Database Indexes")
        popup.geometry("800x600")
        
        # Create text widget with scrollbar
        text_frame = ttk.Frame(popup)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        text = tk.Text(text_frame, wrap=tk.WORD, yscrollcommand=scrollbar.set)
        text.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=text.yview)
        
        # Insert index information for the current database
        current_db = self.db_var.get()
        text.insert(tk.END, f"Indexes for {current_db}:\n\n")
        
        if current_db in INDEX_INFO:
            for index in INDEX_INFO[current_db]:
                # Format the index fields for display
                fields_str = ", ".join([f"{field}: {order}" for field, order in index["fields"]])
                text.insert(tk.END, f"Index: {index['name']}\n")
                text.insert(tk.END, f"Fields: {fields_str}\n")
                text.insert(tk.END, "-" * 50 + "\n\n")
        
        # Add explanation of index usage
        text.insert(tk.END, "\nIndex Usage Guidelines:\n\n")
        
        if current_db == "nzpost_summary_append":
            text.insert(tk.END, "Time Series Collection Indexes:\n")
            text.insert(tk.END, "1. Single field queries:\n")
            text.insert(tk.END, "   - Use tpid_1 for TPID filtering\n")
            text.insert(tk.END, "   - Use timestamp_1 for date range queries\n")
            text.insert(tk.END, "   - Use edifact_code_1 for status filtering\n\n")
            text.insert(tk.END, "2. Combined queries:\n")
            text.insert(tk.END, "   - Use tpid_1_timestamp_1 for TPID + date range\n")
            text.insert(tk.END, "   - Use tpid_1_edifact_code_1 for TPID + status\n")
        else:
            text.insert(tk.END, "Standard Collection Indexes:\n")
            text.insert(tk.END, "1. Single field queries:\n")
            text.insert(tk.END, "   - Use tpid_1 for TPID filtering\n")
            text.insert(tk.END, "   - Use event_datetime_1 for date range queries\n")
            text.insert(tk.END, "   - Use edifact_code_1 for status filtering\n\n")
            text.insert(tk.END, "2. Combined queries:\n")
            text.insert(tk.END, "   - Use tpid_1_event_datetime_1 for TPID + date range\n")
            text.insert(tk.END, "   - Use tpid_1_edifact_code_1 for TPID + status\n")
            text.insert(tk.END, "   - Use tpid_1_edifact_code_1_event_datetime_1 for all three\n")
        
        # Make text read-only
        text.config(state=tk.DISABLED)
        
        # Add close button
        close_button = ttk.Button(popup, text="Close", command=popup.destroy)
        close_button.pack(pady=10)

    def show_pipeline(self):
        """Show the current pipeline in a popup window"""
        # Create a new window
        popup = tk.Toplevel(self.root)
        popup.title("Query Pipeline Details")
        popup.geometry("800x600")
        
        # Create text widget with scrollbar
        text_frame = ttk.Frame(popup)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        text = tk.Text(text_frame, wrap=tk.WORD, yscrollcommand=scrollbar.set, font=("Courier", 10))
        text.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=text.yview)
        
        try:
            # Get the current status
            status = self.active_button.cget("text") if self.active_button else None
            
            # Get selected TPIDs
            selected_tpids = [tpid for tpid, var in self.tpid_vars.items() if var.get()]
            
            # Get date range if enabled
            date_range_str = ""
            if self.use_date_range.get():
                from_date = self.from_date.get_date()
                to_date = self.to_date.get_date()
                date_range_str = f"Date Range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}"
            
            # Get the current query details
            match_stage = self.build_query(status)
            
            # Format the pipeline details based on database type
            if self.current_db == "nzpost_summary_append":
                # For time series database, show latest event pipeline
                
                # Extract date range from match_stage if present
                date_filter = {}
                if "timestamp" in match_stage:
                    date_filter = {"timestamp": match_stage["timestamp"]}
                
                # First filter for TPIDs if selected
                tpid_filter = {}
                if "tpid" in match_stage:
                    tpid_filter = {"tpid": match_stage["tpid"]}
                
                # Build pipeline for latest event
                latest_event_pipeline = [
                    # Match by TPIDs and date range first if specified
                    {"$match": {**tpid_filter, **date_filter}},
                    # Sort by tracking reference and timestamp (descending to get latest first)
                    {"$sort": {"tracking_reference": 1, "timestamp": -1}},
                    # Group by tracking reference and take the first (latest) document
                    {"$group": {
                        "_id": "$tracking_reference",
                        "latest_code": {"$first": "$edifact_code"}
                    }},
                    # Match only those with the specified edifact code
                    {"$match": {"latest_code": EDIFACT_CODES[status] if status else None}},
                    # Count total
                    {"$count": "total"}
                ]
                
                # Determine the hint being used
                hint = self.get_optimal_hint(match_stage)
                
                details = (
                    "Time Series Collection Query\n"
                    "------------------------\n"
                    f"Database: {self.current_db}\n"
                    f"Collection: {COLLECTIONS[self.collection_var.get()]}\n"
                    f"Status: {status}\n"
                    f"Selected TPIDs: {selected_tpids}\n"
                    f"{date_range_str}\n\n"
                    "Pipeline (counts parcels where LATEST status matches criteria):\n"
                    f"{json.dumps(latest_event_pipeline, indent=2, default=str)}\n\n"
                    f"Using Index Hint: {hint}\n\n"
                    "Explanation:\n"
                    "1. First we find all events for the specified TPIDs within the date range\n"
                    "2. Then we sort by tracking reference and timestamp (descending)\n"
                    "3. Group by tracking reference to get the latest event for each parcel\n"
                    f"4. Filter to include only parcels whose latest status is '{status}'\n"
                    "5. Count the resulting unique parcels"
                )
            else:
                # Standard collections just have one pipeline
                standard_pipeline = [
                    {"$match": match_stage},
                    {"$count": "total"}
                ]
                
                # Determine the hint being used
                hint = self.get_optimal_hint(match_stage)
                
                details = (
                    "Standard Collection Query\n"
                    "----------------------\n"
                    f"Database: {self.current_db}\n"
                    f"Collection: {COLLECTIONS[self.collection_var.get()]}\n"
                    f"Status: {status}\n"
                    f"Selected TPIDs: {selected_tpids}\n"
                    f"{date_range_str}\n\n"
                    "Pipeline:\n"
                    f"{json.dumps(standard_pipeline, indent=2, default=str)}\n\n"
                    f"Using Index Hint: {hint}"
                )
            
            text.insert(tk.END, details)
        except Exception as e:
            text.insert(tk.END, f"Error displaying pipeline: {str(e)}")
        
        text.config(state=tk.DISABLED)
        
        # Add close button
        close_button = ttk.Button(popup, text="Close", command=popup.destroy)
        close_button.pack(pady=10)

    def get_optimal_hint(self, match_stage):
        """Determine the optimal index hint based on query conditions"""
        selected_tpids = [tpid for tpid, var in self.tpid_vars.items() if var.get()]
        status = self.active_button.cget("text") if self.active_button else None
        current_db = self.db_var.get()
        
        # Log hint selection process
        print(f"\n=== Index Hint Selection ===")
        print(f"Database: {current_db}")
        print(f"Has TPIDs: {bool(selected_tpids)}")
        print(f"Has Status: {bool(status)}")
        print(f"Has Timestamp: {'timestamp' in match_stage}")
        print(f"Has Event Datetime: {'event_datetime' in match_stage}")
        
        if current_db == "nzpost_summary_append":
            # Time Series Collection - prioritize timestamp for time-based queries
            if "timestamp" in match_stage and selected_tpids and status:
                # All three filters
                hint = "tpid_1_timestamp_1"
                print(f"Selected hint: {hint} - Has all three filters")
            elif selected_tpids and status:
                # TPID and Status only
                hint = "tpid_1_edifact_code_1"
                print(f"Selected hint: {hint} - Has TPID and status")
            elif "timestamp" in match_stage and selected_tpids:
                # TPID and timestamp 
                hint = "tpid_1_timestamp_1"
                print(f"Selected hint: {hint} - Has TPID and timestamp")
            elif "timestamp" in match_stage:
                # Timestamp only - use timestamp index for time series
                hint = "timestamp_1"
                print(f"Selected hint: {hint} - Has timestamp only")
            elif selected_tpids:
                # TPID only
                hint = "tpid_1"
                print(f"Selected hint: {hint} - Has TPID only")
            elif status:
                # Status only
                hint = "edifact_code_1"
                print(f"Selected hint: {hint} - Has status only")
            else:
                # No filters - no hint needed
                hint = None
                print("No hint selected - insufficient filters")
        else:  
            # Standard Collections - nzpost_summary and nzpost_summary_item
            if "event_datetime" in match_stage and selected_tpids and status:
                # All three filters
                hint = "tpid_1_edifact_code_1_event_datetime_1"
                print(f"Selected hint: {hint} - Has all three filters")
            elif selected_tpids and status:
                # TPID and Status only
                hint = "tpid_1_edifact_code_1"
                print(f"Selected hint: {hint} - Has TPID and status")
            elif "event_datetime" in match_stage and selected_tpids:
                # TPID and date range
                hint = "tpid_1_event_datetime_1"
                print(f"Selected hint: {hint} - Has TPID and date range")
            elif "event_datetime" in match_stage:
                # Date range only
                hint = "event_datetime_1"
                print(f"Selected hint: {hint} - Has date range only")
            elif selected_tpids:
                # TPID only
                hint = "tpid_1"
                print(f"Selected hint: {hint} - Has TPID only")
            elif status:
                # Status only
                hint = "edifact_code_1"
                print(f"Selected hint: {hint} - Has status only")
            else:
                # No filters - no hint needed
                hint = None
                print("No hint selected - insufficient filters")
        
        # For time series collections, verify that the timestamp index is correct
        if current_db == "nzpost_summary_append" and "timestamp" in match_stage:
            if hint not in ["timestamp_1", "tpid_1_timestamp_1"]:
                print(f"WARNING: Time series query with timestamp is not using a timestamp index! Using {hint}")
        
        return hint

    def show_performance_pipeline(self):
        """Show the performance test pipeline details in a popup window"""
        # Create a new window
        popup = tk.Toplevel(self.root)
        popup.title("Performance Test Pipeline Details")
        popup.geometry("800x600")
        
        # Create text widget with scrollbar
        text_frame = ttk.Frame(popup)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        text = tk.Text(text_frame, wrap=tk.WORD, yscrollcommand=scrollbar.set, font=("Courier", 10))
        text.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=text.yview)
        
        # Build the test details for each collection
        test_tpids = [1000011, 1000012, 1000013, 1000014, 1000015]
        test_codes = [500, 600]  # Delivered and Attempted Delivery
        current_db = self.db_var.get()
        
        # Date ranges for collections
        collection_date_ranges = {
            "summary_1_week": (datetime(2025, 3, 1), datetime(2025, 3, 7)),
            "summary_2_weeks": (datetime(2025, 3, 1), datetime(2025, 3, 14)),
            "summary_1_month": (datetime(2025, 3, 1), datetime(2025, 3, 31)),
            "summary_3_months": (datetime(2025, 1, 1), datetime(2025, 3, 31))
        }
        
        details = []
        details.append(f"\n=== Database: {current_db} ===\n")
        
        for display_name, collection_name in COLLECTIONS.items():
            details.append(f"\n--- Collection: {collection_name} ({display_name}) ---\n")
            
            # Build match stage
            match_stage = {
                "tpid": {"$in": test_tpids},
                "edifact_code": {"$in": test_codes}
            }
            
            # Always add date range for time series collections, or if enabled for regular collections
            if current_db == "nzpost_summary_append" or self.use_date_range.get():
                from_date, to_date = collection_date_ranges[collection_name]
                from_date = datetime.combine(from_date, datetime.min.time())
                to_date = datetime.combine(to_date, datetime.max.time())
                
                if current_db == "nzpost_summary_append":
                    match_stage["timestamp"] = {
                        "$gte": from_date,
                        "$lte": to_date
                    }
                    details.append(f"Date Range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}\n")
                else:
                    match_stage["event_datetime"] = {
                        "$gte": from_date,
                        "$lte": to_date
                    }
                    if self.use_date_range.get():
                        details.append(f"Date Range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}\n")
            
            # Determine index hint
            hint = None
            if current_db == "nzpost_summary_append":
                if "timestamp" in match_stage:
                    hint = "tpid_1_timestamp_1"
                else:
                    hint = "tpid_1_edifact_code_1"
            else:
                if "event_datetime" in match_stage:
                    hint = "tpid_1_edifact_code_1_event_datetime_1"
                else:
                    hint = "tpid_1_edifact_code_1"
            
            # Calculate timeout based on collection size for time series
            if current_db == "nzpost_summary_append":
                if collection_name == "summary_3_months":
                    timeout_ms = 120000  # 120 seconds for 3 months collection
                elif collection_name == "summary_1_month":
                    timeout_ms = 60000   # 60 seconds for 1 month collection
                elif collection_name == "summary_2_weeks":
                    timeout_ms = 45000   # 45 seconds for 2 weeks collection
                else:
                    timeout_ms = 30000   # 30 seconds for 1 week collection
                details.append(f"Query Timeout: {timeout_ms/1000} seconds\n")
            
            # Build pipeline
            if current_db == "nzpost_summary_append":
                # For time series, we use a pipeline to get parcels with latest event matching criteria
                latest_parcels_pipeline = [
                    # Match TPIDs first to limit dataset
                    {"$match": {"tpid": {"$in": test_tpids}, "timestamp": {"$gte": from_date, "$lte": to_date}}},
                    # Sort by tracking reference and timestamp (descending to get latest first)
                    {"$sort": {"tracking_reference": 1, "timestamp": -1}},
                    # Group by tracking reference and take the first (latest) document
                    {"$group": {
                        "_id": "$tracking_reference",
                        "latest_code": {"$first": "$edifact_code"}
                    }},
                    # Match only those with the specified edifact codes
                    {"$match": {"latest_code": {"$in": test_codes}}},
                    # Count total
                    {"$count": "total"}
                ]
                
                details.append("Pipeline (counts parcels where LATEST status is Delivered/Attempted):")
                details.append(json.dumps(latest_parcels_pipeline, indent=2, default=str))
                details.append(f"\nUsing Index Hint: {hint}")
                
                details.append("\nExplanation:")
                details.append("1. First we find all events for the specified TPIDs within the date range")
                details.append("2. Then we sort by tracking reference and timestamp (descending)")
                details.append("3. Group by tracking reference to get the latest event for each parcel")
                details.append("4. Finally filter to include only parcels whose latest status is Delivered or Attempted")
                details.append("5. Count the resulting unique parcels")
            else:
                # Standard collections just have one pipeline
                pipeline = [
                    {"$match": match_stage},
                    {"$count": "total"}
                ]
                
                details.append("Pipeline:")
                details.append(json.dumps(pipeline, indent=2, default=str))
                details.append(f"\nUsing Index Hint: {hint}")
            
            details.append("-" * 50)
        
        # Insert all details into text widget
        text.insert(tk.END, "\n".join(details))
        text.config(state=tk.DISABLED)
        
        # Add close button
        close_button = ttk.Button(popup, text="Close", command=popup.destroy)
        close_button.pack(pady=10)

if __name__ == "__main__":
    root = tk.Tk()
    app = MongoQueryApp(root)
    root.mainloop() 