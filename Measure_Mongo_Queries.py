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

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['nzpost_summary']

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
SAMPLE_JSON = '''{
  "_id": {
    "$oid": "67ef08ea7e382fec192d35d3"
  },
  "tracking_reference": "NZ100000001",
  "tpid": 1000011,
  "edifact_code": 500,
  "event_description": "Delivered",
  "event_datetime": {
    "$date": "2025-03-06T04:18:28.000Z"
  }
}'''

class MongoQueryApp:
    def __init__(self, root):
        self.root = root
        self.root.title("MongoDB Query Performance Measurement")
        self.root.geometry("1200x1200")  # Increased height to 1200
        
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
        
        self.db_label = ttk.Label(self.connection_frame, text=f"Database: nzpost_summary")
        self.db_label.pack(side=tk.LEFT)
        
        self.status_label = ttk.Label(self.connection_frame, text="Connected", foreground="green")
        self.status_label.pack(side=tk.LEFT, padx=10)
        
        # Add Sample JSON button
        self.sample_json_btn = ttk.Button(self.connection_frame, text="Sample JSON",
                                        command=self.show_sample_json)
        self.sample_json_btn.pack(side=tk.LEFT, padx=10)
        
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
        ttk.Checkbutton(self.date_frame, text="Use Date Range", variable=self.use_date_range, 
                       command=self.toggle_date_pickers).pack(anchor=tk.W)
        
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
        
        self.performance_btn.bind("<Enter>", self.show_performance_details)
        self.performance_btn.bind("<Leave>", self.hide_query_details)
        
        # Right side: Performance Test Query Display
        self.perf_query_frame = ttk.Frame(self.query_frame)
        self.perf_query_frame.pack(side=tk.LEFT, padx=20, fill=tk.X, expand=True)
        
        self.perf_query_label = ttk.Label(self.perf_query_frame, 
                                        text="Performance Test Aggregation Pipeline:",
                                        justify=tk.LEFT)
        self.perf_query_label.pack(anchor=tk.W)
        
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
    
    def show_query_details(self, event, status=None):
        details = self.get_query_details(status)
        tooltip = tk.Toplevel(self.root)
        tooltip.wm_overrideredirect(True)
        tooltip.wm_geometry(f"+{event.x_root+10}+{event.y_root+10}")
        label = ttk.Label(tooltip, text=details, justify=tk.LEFT,
                         background="#ffffe0", relief='solid', borderwidth=1)
        label.pack()
        self.tooltip = tooltip
    
    def hide_query_details(self, event):
        if hasattr(self, 'tooltip'):
            self.tooltip.destroy()
    
    def get_query_details(self, status=None) -> str:
        details = []
        details.append(f"Collection: {COLLECTIONS[self.collection_var.get()]}")
        
        selected_tpids = [tpid for tpid, var in self.tpid_vars.items() if var.get()]
        details.append(f"TPIDs: {', '.join(map(str, selected_tpids))}")
        
        if status:
            details.append(f"Edifact Code: {EDIFACT_CODES[status]} - {status}")
        
        if self.use_date_range.get():
            from_date = self.from_date.get_date()
            to_date = self.to_date.get_date()
            details.append(f"Date Range: {from_date} to {to_date}")
        
        return "\n".join(details)
    
    def build_query(self, status=None) -> Dict:
        query = {}
        
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
            query["event_datetime"] = {"$gte": from_date, "$lte": to_date}
        
        return query
    
    def run_status_query(self, status):
        collection = db[COLLECTIONS[self.collection_var.get()]]
        match_stage = self.build_query(status)
        
        # Get selected TPIDs for query
        selected_tpids = [tpid for tpid, var in self.tpid_vars.items() if var.get()]
        tpid_query = {"tpid": {"$in": selected_tpids}}
        
        # Pipeline for total parcels count
        pipeline_total = [
            {"$match": tpid_query},
            {"$count": "total"}
        ]
        
        # Execute total parcels query
        total_result = list(collection.aggregate(
            pipeline_total,
            allowDiskUse=False,
            hint="tpid_1"
        ))
        total_parcels = total_result[0]["total"] if total_result else 0
        self.parcels_label.config(text=f"Parcels: {total_parcels:,}")
        
        # Determine optimal index based on query conditions
        hint = None
        if "event_datetime" in match_stage and selected_tpids and status:
            hint = "tpid_1_edifact_code_1_event_datetime_1"
        elif selected_tpids and status:
            hint = "tpid_1_edifact_code_1"
        elif "event_datetime" in match_stage and selected_tpids:
            hint = "tpid_1_event_datetime_1"
        elif "event_datetime" in match_stage and status:
            hint = "edifact_code_1_event_datetime_1"
        elif "event_datetime" in match_stage:
            hint = "event_datetime_1"
        elif selected_tpids:
            hint = "tpid_1"
        elif status:
            hint = "edifact_code_1"
        
        # Build and execute the main query pipeline
        pipeline = [
            {"$match": match_stage},
            {"$count": "total"}
        ]
        
        # Update query details display
        query_text = f"Aggregation Pipeline:\n{pipeline}\n\nUsing index hint: {hint}"
        self.query_label.config(text=query_text)
        
        # Execute query and measure performance
        start_time = time.time()
        result = list(collection.aggregate(
            pipeline,
            allowDiskUse=False,
            hint=hint
        ))
        end_time = time.time()
        
        # Update results display
        count = result[0]["total"] if result else 0
        self.count_label.config(text=f"Count: {count:,}")
        self.time_label.config(text=f"Response Time: {(end_time - start_time) * 1000:.2f}ms")
    
    def run_performance_test(self):
        test_tpids = [1000011, 1000012, 1000013, 1000014, 1000015]
        test_codes = [500, 600]  # Delivered and Attempted Delivery
        
        results = {}
        for collection_name, collection in COLLECTIONS.items():
            match_stage = {
                "tpid": {"$in": test_tpids},
                "edifact_code": {"$in": test_codes}
            }
            
            # Only add date range filter if enabled
            if self.use_date_range.get():
                match_stage["event_datetime"] = {
                    "$gte": datetime.combine(self.from_date.get_date(), datetime.min.time()),
                    "$lte": datetime.combine(self.to_date.get_date(), datetime.max.time())
                }
            
            # Determine which index to use based on the query
            hint = None
            if "event_datetime" in match_stage:
                # If we have date range, use a compound index
                hint = "tpid_1_edifact_code_1_event_datetime_1"
            else:
                # If we only have TPID and edifact code
                hint = "tpid_1_edifact_code_1"
            
            # Optimize the pipeline for counting
            pipeline = [
                {"$match": match_stage},
                {"$count": "total"}
            ]
            
            # Update performance test query display
            query_text = (
                f"Collection: {collection_name}\n"
                f"Aggregation Pipeline:\n{pipeline}\n"
                f"Using index hint: {hint}"
            )
            self.perf_query_label.config(text=query_text)
            
            start_time = time.time()
            result = list(db[collection].aggregate(
                pipeline,
                allowDiskUse=False,  # Force in-memory execution
                hint=hint  # Use the appropriate index
            ))
            end_time = time.time()
            
            count = result[0]["total"] if result else 0
            results[collection_name] = {
                "time": (end_time - start_time) * 1000,
                "count": count
            }
        
        # Create bar graph
        self.plot_results(results)
    
    def plot_results(self, results: Dict):
        # Clear previous graph
        for widget in self.graph_frame.winfo_children():
            widget.destroy()
        
        fig, ax = plt.subplots(figsize=(12, 10))  # Increased figure height from 8 to 10
        collections = list(results.keys())
        times = [results[col]["time"] for col in collections]
        counts = [results[col]["count"] for col in collections]
        
        bars = ax.bar(collections, times)
        ax.set_ylabel('Response Time (ms)')
        ax.set_title('Query Performance Across Collections')
        
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=15, ha='right')
        
        # Add count labels above bars
        for bar, count in zip(bars, counts):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 5,
                   f'{count:,}',
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
        details = []
        details.append("Performance Test Query Details:")
        details.append("TPIDs: 1000011, 1000012, 1000013, 1000014, 1000015")
        details.append("Edifact Codes: 500 - Delivered, 600 - Attempted Delivery")
        
        if self.use_date_range.get():
            from_date = self.from_date.get_date()
            to_date = self.to_date.get_date()
            details.append(f"Date Range: {from_date} to {to_date}")
        
        details.append("\nWill run on all collections:")
        for collection_name in COLLECTIONS.keys():
            details.append(f"- {collection_name}")
        
        tooltip = tk.Toplevel(self.root)
        tooltip.wm_overrideredirect(True)
        tooltip.wm_geometry(f"+{event.x_root+10}+{event.y_root+10}")
        label = ttk.Label(tooltip, text="\n".join(details), justify=tk.LEFT,
                         background="#ffffe0", relief='solid', borderwidth=1)
        label.pack()
        self.tooltip = tooltip

    def show_tpid_volume(self, event, tpid):
        """Show tooltip with TPID volume information."""
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
    
    def hide_tooltip(self, event=None):
        """Hide the current tooltip if it exists."""
        if hasattr(self, 'current_tooltip'):
            self.current_tooltip.destroy()
            del self.current_tooltip

    def show_sample_json(self):
        """Show a popup window with sample JSON document structure."""
        # Create popup window
        popup = tk.Toplevel(self.root)
        popup.title("Sample Document Structure")
        popup.geometry("500x300")
        
        # Make the window float on top
        popup.transient(self.root)
        popup.grab_set()
        
        # Create text widget with JSON content
        text_widget = tk.Text(popup, wrap=tk.NONE, font=("Courier", 10))
        text_widget.insert("1.0", SAMPLE_JSON)
        text_widget.config(state="disabled")  # Make it read-only
        
        # Add scrollbars
        y_scrollbar = ttk.Scrollbar(popup, orient="vertical", command=text_widget.yview)
        x_scrollbar = ttk.Scrollbar(popup, orient="horizontal", command=text_widget.xview)
        text_widget.configure(yscrollcommand=y_scrollbar.set, xscrollcommand=x_scrollbar.set)
        
        # Pack everything
        text_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Add close button
        close_btn = ttk.Button(popup, text="Close", command=popup.destroy)
        close_btn.pack(pady=10)

if __name__ == "__main__":
    root = tk.Tk()
    app = MongoQueryApp(root)
    root.mainloop() 