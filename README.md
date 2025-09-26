# Chat Analytics Dashboard

A real-time web dashboard for analyzing chat logs with automatic updates, duplicate user detection, and stylometry analysis.

## Features

- **Real-time Updates**: Automatically processes new chat data every 60 seconds
- **Incremental Processing**: Only reads new messages from log files, skipping already processed data
- **SQLite Database**: Stores all chat data for fast querying and analysis
- **Web Dashboard**: Modern HTML interface with filtering, sorting, and real-time updates
- **Alt Account Detection**: Uses both temporal analysis and stylometry to identify potential duplicate users
- **Date Filtering**: Filter data by specific dates or date ranges
- **WebSocket Updates**: Real-time data updates without page refresh

## Installation

1. **Install Python Requirements**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Ensure Log File Structure**:
   ```
   Channels/
   ├── channel1/
   │   ├── channel1-2025-09-16.log
   │   ├── channel1-2025-09-17.log
   │   └── ...
   ├── channel2/
   │   ├── channel2-2025-09-16.log
   │   └── ...
   └── ...
   ```

3. **Log File Format**:
   Each log file should contain chat messages in this format:
   ```
   [HH:MM:SS] username: message content
   [15:28:55] john_doe: Hello everyone!
   [15:29:01] jane_smith: Hey there!
   ```

## Usage

### Starting the Server

**Option 1: Using the runner script (recommended)**:
```bash
python run_server.py
```

**Option 2: Direct Flask execution**:
```bash
python app.py
```

The dashboard will be available at: `http://localhost:5000`

### Dashboard Features

1. **Channel Overview**: Cards showing summary statistics for each channel
2. **Real-time Updates**: Data refreshes automatically every 60 seconds
3. **Date Filtering**: 
   - Single date: `2025-09-16`
   - Date range: `2025-09-16:2025-09-17`
4. **User Analytics**:
   - Chat message counts
   - Alt likelihood percentages
   - Similar users based on writing style
5. **Filtering & Sorting**: Search users and sort by different criteria

### API Endpoints

- `GET /`: Main dashboard
- `GET /api/channels`: List all channels
- `GET /api/channel/<name>`: Get channel data with optional date filtering
- `GET /api/summary`: Get summary of all channels
- `GET /api/process`: Manually trigger data processing

## How It Works

### Data Processing Pipeline

1. **File Monitoring**: Tracks processed files and their last modification times
2. **Incremental Reading**: Only processes new lines from log files
3. **Message Parsing**: Extracts timestamp, username, and message content
4. **Database Storage**: Stores messages in SQLite for fast querying
5. **Analytics Generation**: 
   - Calculates chat counts per user
   - Performs stylometry analysis using TF-IDF vectorization
   - Groups similar users based on writing patterns
   - Estimates unique user count

### Alt Account Detection

The system uses two methods to detect potential alternate accounts:

1. **Temporal Analysis**: Groups users who never chat simultaneously (within 2 seconds)
2. **Stylometry Analysis**: Uses TF-IDF vectorization and cosine similarity to identify users with similar writing styles

Alt likelihood is calculated as a percentage based on the highest similarity score with other users.

### Database Schema

- `chat_messages`: Individual chat messages with timestamps
- `processed_files`: Tracks which files have been processed and up to which line
- `user_stats`: Cached user statistics including alt likelihood
- `stylometry_groups`: Groups of users identified as potentially the same person

## Configuration

### Updating Interval

To change the update interval from 60 seconds, modify the scheduler configuration in `app.py`:

```python
scheduler.add_job(
    func=process_and_update,
    trigger=IntervalTrigger(seconds=30),  # Change to 30 seconds
    # ...
)
```

### Similarity Threshold

To adjust the alt account detection sensitivity, modify the `similarity_threshold` in the stylometry analysis:

```python
groups, alt_scores, similar_users = group_users_by_stylometry(
    user_messages, 
    similarity_threshold=0.6  # Lower = more sensitive
)
```

## File Structure

```
├── app.py                 # Main Flask application
├── database.py            # Database operations
├── chat_processor.py      # Chat log processing logic
├── run_server.py          # Server runner script
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── templates/
│   └── dashboard.html    # Web dashboard template
└── Channels/             # Chat log directories
    ├── channel1/
    ├── channel2/
    └── ...
```

## Troubleshooting

### Common Issues

1. **No data showing**: 
   - Check if log files exist in the `Channels/` directory
   - Verify log file format matches expected pattern
   - Check browser console for JavaScript errors

2. **Database errors**:
   - Delete `chat_data.db` file to reset the database
   - Restart the server to reinitialize

3. **Performance issues**:
   - Large log files may take time to process initially
   - Consider adjusting the update interval for better performance

### Logs and Debugging

The server outputs processing information to the console:
- Connection status
- Number of new messages processed
- Processing errors
- WebSocket connections

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is provided as-is for educational and analysis purposes.