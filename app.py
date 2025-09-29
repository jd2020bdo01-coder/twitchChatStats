from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit
import threading
from datetime import datetime
from chat_processor import ChatProcessor

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global processor instance
processor = ChatProcessor()

# Background scheduler for periodic updates
scheduler = BackgroundScheduler()

def process_and_update():
    """Background task to process new chat data and update analytics"""
    try:
        print(f"[{datetime.now()}] Starting periodic update...")
        
        # Process all channels for new messages
        results = processor.process_all_channels()
        
        total_new_messages = 0
        for channel, (messages, files) in results.items():
            total_new_messages += messages
            if messages > 0:
                print(f"  {channel}: {messages} new messages from {files} files")
                # Update analytics for this channel
                processor.update_user_analytics(channel)
        
        if total_new_messages > 0:
            print(f"  Total: {total_new_messages} new messages processed")
            # Emit update to all connected clients
            summary = processor.get_all_channels_summary()
            socketio.emit('data_update', {'channels': summary}, broadcast=True)
        else:
            print("  No new messages to process")
            
    except Exception as e:
        print(f"Error in periodic update: {e}")

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/api/channels')
def get_channels():
    """Get list of all channels"""
    try:
        channels = processor.db.get_channels()
        return jsonify({'channels': channels})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/channel/<channel_name>/dates')
def get_channel_dates(channel_name):
    """Get available dates for a specific channel"""
    try:
        dates = processor.db.get_available_dates(channel_name)
        return jsonify({'dates': dates})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/channel/<channel_name>')
def get_channel_data(channel_name):
    """Get data for a specific channel"""
    try:
        date_filter = request.args.get('date_filter')
        
        # If date filter is provided, get filtered data
        if date_filter:
            chat_counts = processor.db.get_user_chat_counts(channel_name, date_filter)
            user_messages = processor.db.get_user_messages(channel_name, date_filter)
            
            # Perform stylometry analysis on filtered data
            groups, alt_scores, similar_users = processor.group_users_by_stylometry(user_messages)
            
            # Convert to user stats format
            user_stats = []
            for username in chat_counts:
                chat_count = chat_counts[username]
                alt_likelihood = alt_scores.get(username, 0.0) * 100
                similar_user_list = similar_users.get(username, [])
                
                user_stats.append({
                    'username': username,
                    'chat_count': chat_count,
                    'alt_likelihood': alt_likelihood,
                    'similar_users': similar_user_list,
                    'last_updated': datetime.now().isoformat()
                })
            
            # Sort by chat count
            user_stats.sort(key=lambda x: x['chat_count'], reverse=True)
            
            # Get date range for filtered data
            if ':' in date_filter:
                start_date, end_date = date_filter.split(':')
            else:
                start_date = end_date = date_filter
            
            return jsonify({
                'channel': channel_name,
                'user_stats': user_stats,
                'start_date': start_date,
                'end_date': end_date,
                'unique_user_count': len(groups),
                'total_users': len(user_stats),
                'total_messages': sum(stat['chat_count'] for stat in user_stats),
                'last_updated': datetime.now().isoformat()
            })
        else:
            # Get cached data from database
            summary = processor.get_channel_summary(channel_name)
            return jsonify(summary)
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/summary')
def get_summary():
    """Get summary of all channels"""
    try:
        summary = processor.get_all_channels_summary()
        return jsonify({'channels': summary})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/user/<username>')
def get_user_details(username):
    """Get detailed information about a specific user"""
    try:
        channel = request.args.get('channel')
        date_filter = request.args.get('date_filter')
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 100))
        
        if not channel:
            return jsonify({'error': 'Channel parameter required'}), 400
        
        # Get user's basic stats
        user_stats = processor.db.get_user_stats(channel)
        user_stat = next((stat for stat in user_stats if stat['username'] == username), None)
        
        if not user_stat:
            return jsonify({'error': 'User not found'}), 404
        
        # Get user's messages with pagination
        user_messages = processor.db.get_user_messages_paginated(
            channel, username, date_filter, page, limit
        )
        
        # Get user's channel activity (which channels they're active in)
        user_channels = processor.db.get_user_channels(username)
        
        # Get user's activity timeline
        activity_timeline = processor.db.get_user_activity_timeline(
            channel, username, date_filter
        )
        
        # Get detailed temporal analysis
        temporal_analysis = processor.db.get_user_temporal_analysis(
            channel, username, date_filter
        )
        
        # Get behavioral insights
        behavioral_insights = processor.db.get_user_behavioral_insights(
            channel, username, date_filter
        )
        
        return jsonify({
            'username': username,
            'stats': user_stat,
            'messages': user_messages,
            'channels': user_channels,
            'activity_timeline': activity_timeline,
            'temporal_analysis': temporal_analysis,
            'behavioral_insights': behavioral_insights,
            'pagination': {
                'page': page,
                'limit': limit,
                'has_more': len(user_messages) == limit
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/process')
def manual_process():
    """Manually trigger processing of all channels"""
    try:
        print("Manual processing triggered...")
        results = processor.process_all_channels()
        
        # Update analytics for all channels with new data
        for channel, (messages, files) in results.items():
            if messages > 0:
                processor.update_user_analytics(channel)
        
        summary = processor.get_all_channels_summary()
        
        # Emit update to all connected clients
        socketio.emit('data_update', {'channels': summary}, broadcast=True)
        
        return jsonify({
            'success': True,
            'results': results,
            'channels': summary
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('Client connected')
    # Send current data to newly connected client
    try:
        summary = processor.get_all_channels_summary()
        emit('data_update', {'channels': summary})
    except Exception as e:
        print(f"Error sending initial data: {e}")

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

@socketio.on('request_channel_data')
def handle_channel_request(data):
    """Handle request for specific channel data"""
    try:
        channel_name = data.get('channel')
        date_filter = data.get('date_filter')
        
        if not channel_name:
            emit('error', {'message': 'Channel name required'})
            return
        
        # Get channel data (similar to API endpoint)
        if date_filter:
            chat_counts = processor.db.get_user_chat_counts(channel_name, date_filter)
            user_messages = processor.db.get_user_messages(channel_name, date_filter)
            groups, alt_scores, similar_users = processor.group_users_by_stylometry(user_messages)
            
            user_stats = []
            for username in chat_counts:
                user_stats.append({
                    'username': username,
                    'chat_count': chat_counts[username],
                    'alt_likelihood': alt_scores.get(username, 0.0) * 100,
                    'similar_users': similar_users.get(username, []),
                    'last_updated': datetime.now().isoformat()
                })
            
            user_stats.sort(key=lambda x: x['chat_count'], reverse=True)
            
            if ':' in date_filter:
                start_date, end_date = date_filter.split(':')
            else:
                start_date = end_date = date_filter
            
            channel_data = {
                'channel': channel_name,
                'user_stats': user_stats,
                'start_date': start_date,
                'end_date': end_date,
                'unique_user_count': len(groups),
                'total_users': len(user_stats),
                'total_messages': sum(stat['chat_count'] for stat in user_stats),
                'last_updated': datetime.now().isoformat()
            }
        else:
            channel_data = processor.get_channel_summary(channel_name)
        
        emit('channel_data', channel_data)
        
    except Exception as e:
        emit('error', {'message': str(e)})

if __name__ == '__main__':
    # Initialize database and process existing data
    print("Initializing application...")
    try:
        # Check if we have any existing data to speed up startup
        existing_channels = processor.db.get_channels()
        has_existing_data = len(existing_channels) > 0
        
        if has_existing_data:
            print(f"Found existing data for {len(existing_channels)} channels")
            print("Checking for new messages...")
        else:
            print("No existing data found, performing initial processing...")
        
        # Process chat logs (this will only process new/changed files)
        results = processor.process_all_channels()
        total_new_messages = sum(messages for messages, files in results.values())
        
        if total_new_messages > 0:
            print(f"Processed {total_new_messages} new messages from logs")
        else:
            print("No new messages to process")
        
        # Only update analytics for channels that need it
        channels = processor.db.get_channels()
        analytics_needed = 0
        
        for channel in channels:
            new_msg_count = results.get(channel, (0, 0))[0]
            if processor.needs_analytics_update(channel, new_msg_count):
                print(f"Updating analytics for {channel}...")
                processor.update_user_analytics(channel)
                analytics_needed += 1
            else:
                print(f"Analytics for {channel} are up to date, skipping...")
        
        if analytics_needed == 0:
            print("All analytics are up to date! ✅")
        else:
            print(f"Updated analytics for {analytics_needed} channels")
        
        print("Initial processing complete!")
        
    except Exception as e:
        print(f"Error during initialization: {e}")
    
    # Start the background scheduler
    scheduler.add_job(
        func=process_and_update,
        trigger=IntervalTrigger(seconds=60),  # Run every 60 seconds
        id='process_chats',
        name='Process chat logs and update analytics',
        replace_existing=True
    )
    scheduler.start()
    
    # Shut down the scheduler when exiting the app
    atexit.register(lambda: scheduler.shutdown())
    
    print("Starting Flask-SocketIO server...")
    print("Dashboard will be available at: http://localhost:5001")
    
    # Run the Flask-SocketIO server
    socketio.run(app, debug=False, host='0.0.0.0', port=5001)