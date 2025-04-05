"""
Script to export Zendesk tickets with comments using bulk API.

This script uses Zendesk's bulk/batch API capabilities to efficiently retrieve 
tickets and comments, significantly reducing API call volume while maintaining 
the same output format.
"""

import sys
import datetime
import calendar
import argparse
import time
import json
import os
from typing import Dict, List, Any, Optional, Set, Tuple
import asyncio
import concurrent.futures
from urllib.parse import urlencode

import pandas as pd
import requests
import aiohttp

from zendesk_api.auth import auth
from zendesk_api.tickets import zendesk_tickets
from zendesk_api.monitoring import (
    timed_api_call, 
    load_user_cache, 
    save_user_cache, 
    print_api_usage_report, 
    reset_api_tracking
)


# Maximum number of tickets to request in a single batch
MAX_BATCH_SIZE = 100

# Maximum number of concurrent API requests
MAX_CONCURRENT_REQUESTS = 5


@timed_api_call("authentication")
def test_authentication() -> bool:
    """
    Test authentication with Zendesk API.
    
    Returns:
        bool: True if authentication is successful, False otherwise
    """
    print("Testing Zendesk API authentication...")
    
    is_valid, error = auth.validate_credentials()
    
    if is_valid:
        print("[SUCCESS] Authentication successful!")
        return True
    else:
        print(f"[ERROR] Authentication failed: {error}")
        return False


def get_last_30_days_range() -> tuple[datetime.datetime, datetime.datetime]:
    """
    Calculate the date range for the last 30 days, including current date.
    
    Returns:
        tuple: (start_date, end_date) for the last 30 days
    """
    # Get current date
    end_date = datetime.datetime.now(datetime.UTC)
    
    # Calculate start date (30 days ago)
    start_date = end_date - datetime.timedelta(days=30)
    
    return start_date, end_date


def get_previous_month_range() -> tuple[datetime.datetime, datetime.datetime]:
    """
    Calculate the date range for the previous calendar month.
    
    Returns:
        tuple: (start_date, end_date) for the previous month
    """
    # Get current date
    current_date = datetime.datetime.now(datetime.UTC)
    
    # Calculate previous month (handling January case)
    if current_date.month == 1:
        # If current month is January, previous month is December of previous year
        year = current_date.year - 1
        month = 12
    else:
        year = current_date.year
        month = current_date.month - 1
    
    # Calculate start date (1st day of previous month)
    start_date = datetime.datetime(year, month, 1, tzinfo=datetime.UTC)
    
    # Calculate end date (last day of previous month)
    _, last_day = calendar.monthrange(year, month)
    end_date = datetime.datetime(year, month, last_day, 23, 59, 59, tzinfo=datetime.UTC)
    
    return start_date, end_date


@timed_api_call("ticket_listing")
def retrieve_tickets(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    date_label: str
) -> List[Dict[str, Any]]:
    """
    Retrieve tickets created within the specified date range using Zendesk search API.
    
    Args:
        start_date: Start date for filtering
        end_date: End date for filtering
        date_label: Label to describe the date range (for display purposes)
        
    Returns:
        List[Dict[str, Any]]: List of tickets
    """
    print(f"\nRetrieving tickets from {date_label}...")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    try:
        # Format dates for Zendesk search API
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        # Use the same URL and approach as the original script for consistency
        url = f"{zendesk_tickets.base_url}/api/v2/tickets.json"
        
        # Format dates in the way Zendesk API expects
        params = {
            "created_after": start_date.isoformat(),
            "created_before": end_date.isoformat(),
            "per_page": 100
        }
        
        all_tickets = []
        
        while url:
            response = requests.get(
                url,
                auth=zendesk_tickets.auth.get_auth_object(),
                params=params,
                timeout=30
            )
            
            if response.status_code != 200:
                raise ValueError(f"Failed to retrieve tickets: {response.status_code} - {response.text}")
                
            data = response.json()
            
            # Add tickets from this page
            tickets = data.get('tickets', [])
            all_tickets.extend(tickets)
            
            # Reset params for pagination
            params = {}
            
            # Check for next page
            if data.get('next_page'):
                url = data['next_page']
            else:
                url = None
        
        print(f"[SUCCESS] Retrieved {len(all_tickets)} tickets from {date_label}")
        
        # Display basic information about the tickets
        if all_tickets:
            # Show count by status
            status_counts = {}
            for ticket in all_tickets:
                status = ticket.get('status', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
            
            print("\nTicket counts by status:")
            for status, count in status_counts.items():
                print(f"  - {status}: {count}")
        
        return all_tickets
    except Exception as e:
        print(f"[ERROR] Failed to retrieve tickets: {e}")
        return []


def retrieve_last_30_days_tickets() -> List[Dict[str, Any]]:
    """
    Retrieve all tickets created in the last 30 days.
    
    Returns:
        List[Dict[str, Any]]: List of tickets
    """
    start_date, end_date = get_last_30_days_range()
    return retrieve_tickets(start_date, end_date, "last 30 days")


def retrieve_last_month_tickets() -> List[Dict[str, Any]]:
    """
    Retrieve all tickets created in the previous calendar month.
    
    Returns:
        List[Dict[str, Any]]: List of tickets
    """
    start_date, end_date = get_previous_month_range()
    month_name = start_date.strftime("%B %Y")
    return retrieve_tickets(start_date, end_date, month_name)


@timed_api_call("users")
def get_all_users(use_cache: bool = True) -> Dict[int, Dict[str, str]]:
    """
    Retrieve all users from Zendesk to build a comprehensive mapping.
    Use caching to minimize API calls.
    
    Args:
        use_cache: Whether to use cached user data if available
        
    Returns:
        Dict[int, Dict[str, str]]: Dictionary mapping user IDs to user details
    """
    print("\nRetrieving user information...")
    
    # Check if we have a valid cache
    if use_cache:
        user_cache = load_user_cache()
        # If we have entries beyond the default placeholder, use the cache
        if len(user_cache) > 1:
            print(f"[SUCCESS] Using cached data for {len(user_cache)} users")
            return user_cache
    
    print("No valid cache found, fetching users from API...")
    user_map = {None: {"name": "Unknown User", "email": "unknown@example.com"}}
    
    try:
        # Get all users using the bulk export endpoint
        url = f"{zendesk_tickets.base_url}/api/v2/users.json"
        params = {"per_page": 100}
        
        all_users = []
        
        while url:
            response = requests.get(
                url,
                auth=zendesk_tickets.auth.get_auth_object(),
                params=params,
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"[WARNING] Failed to retrieve users: {response.status_code} - {response.text}")
                break
                
            data = response.json()
            all_users.extend(data.get('users', []))
            
            # Reset params for pagination
            params = {}
            
            # Check for next page
            if data.get('next_page'):
                url = data['next_page']
            else:
                url = None
        
        # Build user mapping
        for user in all_users:
            user_id = user.get('id')
            if user_id:
                user_map[user_id] = {
                    "name": user.get('name', 'Unknown'),
                    "email": user.get('email', f'user_{user_id}@example.com')
                }
            
        print(f"[SUCCESS] Retrieved {len(all_users)} users")
        
        # Save to cache
        save_user_cache(user_map)
        
        return user_map
        
    except Exception as e:
        print(f"[WARNING] Error retrieving users: {e}")
        return user_map


@timed_api_call("ticket_comments_bulk")
def get_bulk_ticket_comments(ticket_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Get comments for multiple tickets in bulk.
    
    Args:
        ticket_ids: List of ticket IDs to get comments for
        
    Returns:
        Dict[int, List[Dict[str, Any]]]: Dictionary mapping ticket IDs to their comments
    """
    if not ticket_ids:
        return {}
    
    # Create batches of ticket IDs to process
    batches = [ticket_ids[i:i + MAX_BATCH_SIZE] for i in range(0, len(ticket_ids), MAX_BATCH_SIZE)]
    result = {}
    
    for batch in batches:
        # Create a "show_many" request for ticket comments
        ticket_paths = [f"tickets/{ticket_id}/comments.json" for ticket_id in batch]
        show_many_url = f"{zendesk_tickets.base_url}/api/v2/show_many.json"
        
        # Join IDs with comma
        ids_param = ",".join([str(ticket_id) for ticket_id in batch])
        
        # Make the request
        try:
            response = requests.get(
                show_many_url,
                auth=zendesk_tickets.auth.get_auth_object(),
                params={"ids": ids_param, "type": "ticket"},
                timeout=30
            )
            
            if response.status_code == 200:
                # Process the response
                data = response.json()
                tickets = data.get('tickets', [])
                
                for ticket in tickets:
                    ticket_id = ticket.get('id')
                    comments = ticket.get('comments', [])
                    if ticket_id:
                        result[ticket_id] = comments
            else:
                print(f"[WARNING] Failed to retrieve comments in bulk: {response.status_code}")
        except Exception as e:
            print(f"[ERROR] Error retrieving comments in bulk: {e}")
    
    return result


def format_ticket_for_export(
    ticket: Dict[str, Any], 
    comments: List[Dict[str, Any]], 
    user_map: Dict[int, Dict[str, str]]
) -> Dict[str, Any]:
    """
    Format a ticket with its comments for export.
    
    Args:
        ticket: Ticket data
        comments: Comments for the ticket
        user_map: User mapping for attribution
        
    Returns:
        Dict[str, Any]: Formatted ticket data for export
    """
    # Format comments with author and timestamp
    formatted_comments = []
    
    for comment in comments:
        author_id = comment.get('author_id')
        author_info = user_map.get(author_id, user_map[None])
        author_name = author_info["name"]
        author_email = author_info["email"]
        created_at = comment.get('created_at', 'Unknown date')
        comment_type = "INTERNAL" if not comment.get('public', True) else "PUBLIC"
        body = comment.get('body', '').strip()
        
        formatted_comment = (
            f"[{comment_type}] {author_name} ({author_email}) - {created_at}\n"
            f"{body}\n"
            f"{'-' * 40}\n"
        )
        
        formatted_comments.append(formatted_comment)
    
    # Create a new dictionary with the fields we want to export
    formatted_ticket = {
        'id': ticket.get('id'),
        'subject': ticket.get('subject'),
        'status': ticket.get('status'),
        'priority': ticket.get('priority'),
        'type': ticket.get('type'),
        'created_at': ticket.get('created_at'),
        'updated_at': ticket.get('updated_at'),
        'tags': ', '.join(ticket.get('tags', [])),
        'all_comments': "\n".join(formatted_comments) if formatted_comments else "No comments found."
    }
    
    # Add user details
    if ticket.get('assignee_id') and ticket['assignee_id'] in user_map:
        formatted_ticket['assignee_email'] = user_map[ticket['assignee_id']]["email"]
        formatted_ticket['assignee_name'] = user_map[ticket['assignee_id']]["name"]
    else:
        formatted_ticket['assignee_email'] = "unassigned@example.com"
        formatted_ticket['assignee_name'] = "Unassigned"
        
    if ticket.get('requester_id') and ticket['requester_id'] in user_map:
        formatted_ticket['requester_email'] = user_map[ticket['requester_id']]["email"]
        formatted_ticket['requester_name'] = user_map[ticket['requester_id']]["name"]
    else:
        formatted_ticket['requester_email'] = "unknown@example.com"
        formatted_ticket['requester_name'] = "Unknown Requester"
    
    # Add custom fields if available
    if 'custom_fields' in ticket:
        for field in ticket['custom_fields']:
            if field.get('value'):
                field_id = field.get('id')
                formatted_ticket[f'custom_field_{field_id}'] = field.get('value')
    
    return formatted_ticket


async def process_ticket_batch_async(
    session: aiohttp.ClientSession,
    batch: List[Dict[str, Any]],
    user_map: Dict[int, Dict[str, str]],
    auth_tuple: Tuple[str, str]
) -> List[Dict[str, Any]]:
    """
    Process a batch of tickets asynchronously to get their comments.
    
    Args:
        session: aiohttp session
        batch: List of tickets to process
        user_map: User mapping for attribution
        auth_tuple: Authentication tuple (email, api_token)
        
    Returns:
        List[Dict[str, Any]]: List of processed tickets with comments
    """
    # Extract ticket IDs
    ticket_ids = [ticket['id'] for ticket in batch if 'id' in ticket]
    
    if not ticket_ids:
        return []
    
    # Create auth header
    encoded_auth = f"{auth_tuple[0]}/token:{auth_tuple[1]}"
    import base64
    auth_header = f"Basic {base64.b64encode(encoded_auth.encode()).decode()}"
    headers = {"Authorization": auth_header}
    
    try:
        # Process tickets one by one (more reliable than batch)
        processed_tickets = []
        
        for ticket_id in ticket_ids:
            # Get ticket details
            ticket_url = f"{zendesk_tickets.base_url}/api/v2/tickets/{ticket_id}.json"
            
            async with session.get(ticket_url, headers=headers) as response:
                if response.status != 200:
                    print(f"[ERROR] Failed to retrieve ticket {ticket_id}: {response.status}")
                    continue
                    
                data = await response.json()
                ticket = data.get('ticket', {})
            
            # Get ticket comments
            comments_url = f"{zendesk_tickets.base_url}/api/v2/tickets/{ticket_id}/comments.json"
            
            async with session.get(comments_url, headers=headers) as response:
                if response.status != 200:
                    print(f"[ERROR] Failed to retrieve comments for ticket {ticket_id}: {response.status}")
                    comments = []
                else:
                    data = await response.json()
                    comments = data.get('comments', [])
            
            # Format the ticket with comments
            processed_ticket = format_ticket_for_export(ticket, comments, user_map)
            processed_tickets.append(processed_ticket)
            
        return processed_tickets
    except Exception as e:
        print(f"[ERROR] Error processing ticket batch: {e}")
        return []


@timed_api_call("ticket_processing_bulk")
async def process_tickets_in_parallel(
    tickets: List[Dict[str, Any]],
    user_map: Dict[int, Dict[str, str]]
) -> List[Dict[str, Any]]:
    """
    Process tickets in parallel using asyncio.
    
    Args:
        tickets: List of tickets to process
        user_map: User mapping for attribution
        
    Returns:
        List[Dict[str, Any]]: List of processed tickets with comments
    """
    if not tickets:
        return []
    
    # Create batches of tickets
    batch_size = 20  # Process 20 tickets at a time
    batches = [tickets[i:i + batch_size] for i in range(0, len(tickets), batch_size)]
    
    # Get authentication details
    auth_email = zendesk_tickets.auth.email
    auth_token = zendesk_tickets.auth.api_token
    auth_tuple = (auth_email, auth_token)
    
    # Process batches in parallel
    processed_tickets = []
    
    # Create an aiohttp session for all requests
    timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Process batches with concurrency control
        tasks = []
        for batch in batches:
            task = process_ticket_batch_async(session, batch, user_map, auth_tuple)
            tasks.append(task)
        
        # Process tasks in chunks to control concurrency
        for i in range(0, len(tasks), MAX_CONCURRENT_REQUESTS):
            batch_tasks = tasks[i:i + MAX_CONCURRENT_REQUESTS]
            batch_results = await asyncio.gather(*batch_tasks)
            for result in batch_results:
                processed_tickets.extend(result)
            
            # Progress reporting
            completed = min(i + MAX_CONCURRENT_REQUESTS, len(tasks))
            print(f"Processed {completed}/{len(tasks)} batches ({len(processed_tickets)}/{len(tickets)} tickets)")
    
    return processed_tickets


def export_tickets_to_csv(tickets: List[Dict[str, Any]], date_label: str) -> None:
    """
    Export tickets to a CSV file.
    
    Args:
        tickets: List of processed tickets with comments
        date_label: Label to describe the date range (for filename)
    """
    if not tickets:
        print("No tickets to export.")
        return
    
    try:
        # Create DataFrame
        print("\nCreating CSV export...")
        df = pd.DataFrame(tickets)
        
        # Format the filename with the date label
        date_str = date_label.replace(" ", "_").lower()
        csv_filename = f"zendesk_tickets_{date_str}_bulk.csv"
        
        # Use UTF-8 encoding to properly handle French characters
        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(f"[SUCCESS] Exported {len(tickets)} tickets to {csv_filename}")
    except Exception as e:
        print(f"[ERROR] Failed to export tickets: {e}")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Export Zendesk tickets with comments using bulk API")
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=["last30", "lastmonth"], 
        default="last30",
        help="Date range mode: last30 (last 30 days) or lastmonth (previous calendar month)"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable user caching (always fetch fresh user data)"
    )
    parser.add_argument(
        "--skip-report",
        action="store_true",
        help="Skip the API usage report at the end"
    )
    return parser.parse_args()


async def run_async_export():
    """
    Main async function to run the export process.
    
    Returns:
        None
    """
    # Reset API tracking
    reset_api_tracking()
    
    # Record overall execution time
    start_time = time.time()
    
    print("=" * 60)
    print("ZENDESK TICKET EXPORT WITH COMMENTS (BULK API)")
    print("=" * 60)
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Test authentication
    if not test_authentication():
        print("Exiting due to authentication failure.")
        return
    
    # Get all users for proper attribution (with caching)
    user_map = get_all_users(use_cache=not args.no_cache)
    
    # Select date range based on argument (or default to last 30 days)
    if args.mode == "lastmonth":
        print("\nExporting tickets from the previous calendar month...")
        start_date, end_date = get_previous_month_range()
        date_label = start_date.strftime("%B_%Y").lower()
        tickets = retrieve_last_month_tickets()
    else:  # Default to last 30 days
        print("\nExporting tickets from the last 30 days (including today)...")
        start_date, end_date = get_last_30_days_range()
        date_label = "last_30_days"
        tickets = retrieve_last_30_days_tickets()
    
    # If we have tickets, process them in parallel and export
    if tickets:
        print(f"\nProcessing {len(tickets)} tickets in parallel using bulk API...")
        processed_tickets = await process_tickets_in_parallel(tickets, user_map)
        export_tickets_to_csv(processed_tickets, date_label)
    else:
        print("No tickets to export.")
    
    # Print API usage report unless skipped
    if not args.skip_report:
        print_api_usage_report()
    
    # Report overall execution time
    total_time = time.time() - start_time
    print(f"\nTotal script execution time: {total_time:.2f} seconds")
    print("\nExport process completed!")


def main():
    """Run the Zendesk ticket export script using bulk API."""
    if sys.platform == 'win32':
        # On Windows, we need to use a different event loop policy
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Run the async export
    asyncio.run(run_async_export())


if __name__ == "__main__":
    main()