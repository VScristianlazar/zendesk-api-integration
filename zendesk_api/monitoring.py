"""
Monitoring module for Zendesk API integration.

This module tracks API usage, timing, and provides caching functionality
to minimize API load.
"""

import time
import json
import os
import datetime
from typing import Dict, Any, Optional, Callable
import functools

# Initialize the API call counters
api_calls = {
    "authentication": 0,
    "ticket_listing": 0,
    "ticket_details": 0,
    "ticket_comments": 0,
    "users": 0,
    "other": 0,
    "total": 0
}

# Store timing information
api_timing = {
    "authentication": [],
    "ticket_listing": [],
    "ticket_details": [],
    "ticket_comments": [],
    "users": [],
    "other": [],
    "total": []
}

# User cache
user_cache = {}
user_cache_timestamp = None
USER_CACHE_EXPIRY = 24 * 60 * 60  # 24 hours in seconds


def track_api_call(category: str, execution_time: float) -> None:
    """
    Track an API call with its category and execution time.
    
    Args:
        category: The category of API call (authentication, users, etc.)
        execution_time: The execution time in seconds
    """
    if category in api_calls:
        api_calls[category] += 1
        api_timing[category].append(execution_time)
    else:
        api_calls["other"] += 1
        api_timing["other"].append(execution_time)
        
    api_calls["total"] += 1
    api_timing["total"].append(execution_time)


def timed_api_call(category: str) -> Callable:
    """
    Decorator to time and track API calls.
    
    Args:
        category: The category of API call
        
    Returns:
        Callable: A decorator function
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                execution_time = time.time() - start_time
                track_api_call(category, execution_time)
        return wrapper
    return decorator


def load_user_cache() -> Dict[int, Dict[str, str]]:
    """
    Load the user cache from disk if available.
    
    Returns:
        Dict[int, Dict[str, str]]: The user cache
    """
    global user_cache, user_cache_timestamp
    
    cache_file = "user_cache.json"
    timestamp_file = "user_cache_timestamp.txt"
    
    if os.path.exists(cache_file) and os.path.exists(timestamp_file):
        # Load the timestamp
        with open(timestamp_file, "r") as f:
            timestamp_str = f.read().strip()
            user_cache_timestamp = float(timestamp_str)
            
        # Check if cache is expired
        if user_cache_timestamp and time.time() - user_cache_timestamp < USER_CACHE_EXPIRY:
            # Load the cache
            with open(cache_file, "r", encoding="utf-8") as f:
                # Convert string keys back to integers
                str_cache = json.load(f)
                user_cache = {int(k) if k != "None" else None: v for k, v in str_cache.items()}
                return user_cache
    
    # Initialize empty cache if not loaded
    user_cache = {None: {"name": "Unknown User", "email": "unknown@example.com"}}
    user_cache_timestamp = None
    return user_cache


def save_user_cache(cache: Dict[int, Dict[str, str]]) -> None:
    """
    Save the user cache to disk.
    
    Args:
        cache: The user cache to save
    """
    global user_cache_timestamp
    
    cache_file = "user_cache.json"
    timestamp_file = "user_cache_timestamp.txt"
    
    # Convert to a serializable format (keys must be strings in JSON)
    str_cache = {str(k): v for k, v in cache.items()}
    
    # Save the cache
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(str_cache, f, ensure_ascii=False, indent=2)
    
    # Save the timestamp
    user_cache_timestamp = time.time()
    with open(timestamp_file, "w") as f:
        f.write(str(user_cache_timestamp))


def get_api_usage_report() -> Dict[str, Any]:
    """
    Generate a report of API usage.
    
    Returns:
        Dict[str, Any]: A report with call counts and timing information
    """
    report = {
        "calls": {k: v for k, v in api_calls.items()},
        "timing": {
            k: {
                "total": sum(v),
                "average": sum(v) / len(v) if v else 0,
                "min": min(v) if v else 0,
                "max": max(v) if v else 0,
                "count": len(v)
            } for k, v in api_timing.items()
        },
        "timestamp": datetime.datetime.now().isoformat(),
    }
    
    return report


def print_api_usage_report() -> None:
    """Print a formatted API usage report to the console."""
    report = get_api_usage_report()
    
    print("\n" + "=" * 60)
    print("ZENDESK API USAGE REPORT")
    print("=" * 60)
    
    print("\nAPI CALLS BY CATEGORY:")
    for category, count in report["calls"].items():
        if category != "total" and count > 0:
            print(f"  - {category.replace('_', ' ').title()}: {count} calls")
    print(f"  TOTAL: {report['calls']['total']} calls")
    
    print("\nTIMING INFORMATION (seconds):")
    total_time = report["timing"]["total"]["total"]
    for category, timing in report["timing"].items():
        if category != "total" and timing["count"] > 0:
            print(f"  - {category.replace('_', ' ').title()}:")
            print(f"    * Total: {timing['total']:.2f}s")
            print(f"    * Average: {timing['average']:.4f}s")
            print(f"    * Range: {timing['min']:.4f}s - {timing['max']:.4f}s")
    
    # Overall stats
    print(f"\nOVERALL EXECUTION TIME: {total_time:.2f} seconds")
    avg_time = report["timing"]["total"]["average"]
    print(f"AVERAGE TIME PER API CALL: {avg_time:.4f} seconds")
    
    # Cache information
    if user_cache_timestamp:
        cache_age = time.time() - user_cache_timestamp
        cache_hours = cache_age / 3600
        print(f"\nUSER CACHE:")
        print(f"  - Entries: {len(user_cache)} users")
        print(f"  - Age: {cache_hours:.1f} hours")
        print(f"  - Expires in: {(USER_CACHE_EXPIRY - cache_age) / 3600:.1f} hours")
    else:
        print("\nUSER CACHE: Not used")
    
    print("=" * 60)


def reset_api_tracking() -> None:
    """Reset all API tracking counters and timers."""
    global api_calls, api_timing
    
    # Reset counters
    for key in api_calls:
        api_calls[key] = 0
        
    # Reset timing
    for key in api_timing:
        api_timing[key] = []


# Initialize by loading any existing cache
load_user_cache()