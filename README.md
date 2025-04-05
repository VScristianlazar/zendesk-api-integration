# Zendesk API Integration

A Python-based solution for interacting with the Zendesk API, focusing on ticket retrieval and export with comments. This project provides two implementations:

1. **Standard Implementation**: Sequential processing of tickets and comments
2. **Bulk API Implementation**: Optimized parallel processing with significantly reduced API calls

## Features

- Retrieve tickets from specified date ranges (last 30 days or previous month)
- Fetch all comments for each ticket (public and internal)
- Map user IDs to actual names and email addresses
- Export results to CSV with proper UTF-8 encoding for French characters
- Monitor and report API usage statistics
- Cache user data to minimize API calls

## Performance Comparison

| Metric | Standard Implementation | Bulk API Implementation | Improvement |
|--------|----------------|----------------|-------------|
| **Total API Calls** | 39 calls | **4 calls** | 90% reduction |
| **Execution Time** | 23.68 seconds | **10.97 seconds** | 54% faster |
| **API Calls Per Ticket** | 2.17 calls/ticket | **0.22 calls/ticket** | 90% reduction |

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/VScristianlazar/zendesk-api-integration.git
   cd zendesk-api-integration
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file with your Zendesk credentials:
   ```
   ZENDESK_SUBDOMAIN=your_subdomain
   ZENDESK_EMAIL=your_email@example.com
   ZENDESK_API_TOKEN=your_api_token
   ```

## Usage

### Standard Implementation

```bash
# Export tickets from the last 30 days
python export_monthly_tickets.py

# Export tickets from the previous calendar month
python export_monthly_tickets.py --mode lastmonth

# Skip API usage report
python export_monthly_tickets.py --skip-report

# Force fresh user data (don't use cache)
python export_monthly_tickets.py --no-cache
```

### Bulk API Implementation

```bash
# Export tickets from the last 30 days (with parallel processing)
python export_monthly_tickets_bulk.py

# Export tickets from the previous calendar month
python export_monthly_tickets_bulk.py --mode lastmonth

# Other options work the same as the standard implementation
python export_monthly_tickets_bulk.py --no-cache --skip-report
```

## Project Structure

```
zendesk-api-integration/
├── zendesk_api/
│   ├── __init__.py
│   ├── auth.py           # Authentication module
│   ├── config.py         # Configuration handling
│   ├── tickets.py        # Ticket operations
│   └── monitoring.py     # API call tracking and monitoring
├── export_monthly_tickets.py      # Standard implementation
├── export_monthly_tickets_bulk.py # Optimized bulk implementation
├── test_zendesk_api.py            # API tests
├── test_ticket_comments.py        # Comment tests
├── requirements.txt               # Dependencies
└── .env.example                   # Example environment variables
```

## Dependencies

- `requests`: For making HTTP requests to the Zendesk API
- `pandas`: For data manipulation and CSV export
- `python-dotenv`: For loading environment variables
- `aiohttp`: For asynchronous HTTP requests (bulk implementation only)

## API Load Optimization Features

1. **User Data Caching**
   - User information cached for 24 hours
   - Eliminates repeated user lookup API calls

2. **Asynchronous Processing** (Bulk implementation)
   - Parallel API requests with controlled concurrency
   - Optimized authentication header reuse

3. **Comprehensive Monitoring**
   - Detailed API call tracking by category
   - Timing analysis for all API operations

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.