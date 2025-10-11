from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit
import threading
import socket
import os
import signal
import sys
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
                
                # Get latest message timestamp for each user
                conn = processor.db.db_manager.get_connection().__enter__()
                cursor = conn.cursor()
                cursor.execute(
                    '''SELECT MAX(timestamp) FROM chat_messages WHERE channel = ? AND username = ?''',
                    (channel_name, username)
                )
                last_msg = cursor.fetchone()[0]
                user_stats.append({
                    'username': username,
                    'chat_count': chat_count,
                    'alt_likelihood': alt_likelihood,
                    'similar_users': similar_user_list,
                    'last_updated': last_msg if last_msg else None
                })
                conn.close()
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
            # Get latest message timestamp for each user
            conn = processor.db.db_manager.get_connection().__enter__()
            cursor = conn.cursor()
            for username in chat_counts:
                chat_count = chat_counts[username]
                alt_likelihood = alt_scores.get(username, 0.0) * 100
                similar_user_list = similar_users.get(username, [])
                cursor.execute(
                    '''SELECT MAX(timestamp) FROM chat_messages WHERE channel = ? AND username = ?''',
                    (channel_name, username)
                )
                last_msg = cursor.fetchone()[0]
                user_stats.append({
                    'username': username,
                    'chat_count': chat_counts[username],
                    'alt_likelihood': alt_scores.get(username, 0.0) * 100,
                    'similar_users': similar_users.get(username, []),
                    'last_updated': last_msg if last_msg else None
                })
            conn.close()
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

def run_initial_processing():
    """Run initial processing in background thread"""
    try:
        print("🔄 Running initial processing in background...")
        
        # Process chat logs (this will only process new/changed files)
        results = processor.process_all_channels()
        total_new_messages = sum(messages for messages, files in results.values())
        
        if total_new_messages > 0:
            print(f"📊 Processed {total_new_messages} new messages from logs")
        else:
            print("✅ No new messages to process")
        
        # Only update analytics for channels that need it
        channels = processor.db.get_channels()
        analytics_needed = 0
        
        for channel in channels:
            new_msg_count = results.get(channel, (0, 0))[0]
            if processor.needs_analytics_update(channel, new_msg_count):
                print(f"📈 Updating analytics for {channel}...")
                processor.update_user_analytics(channel)
                analytics_needed += 1
                
                # Emit real-time updates as each channel is processed
                try:
                    summary = processor.get_all_channels_summary()
                    socketio.emit('data_update', {'channels': summary}, broadcast=True)
                    socketio.emit('processing_status', {
                        'status': 'processing', 
                        'message': f'Updated analytics for {channel}',
                        'progress': f'{analytics_needed}/{len([c for c in channels if processor.needs_analytics_update(c, results.get(c, (0, 0))[0])])}'
                    }, broadcast=True)
                except Exception as emit_error:
                    print(f"Warning: Could not emit update: {emit_error}")
            else:
                print(f"✅ Analytics for {channel} are up to date, skipping...")
        
        if analytics_needed == 0:
            print("🎉 All analytics are up to date!")
        else:
            print(f"✅ Updated analytics for {analytics_needed} channels")
        
        print("🎯 Initial processing complete!")
        
        # Final update after all processing is done
        try:
            summary = processor.get_all_channels_summary()
            socketio.emit('data_update', {'channels': summary}, broadcast=True)
            socketio.emit('processing_status', {
                'status': 'complete',
                'message': 'All processing complete!',
                'progress': '100%'
            }, broadcast=True)
        except Exception as emit_error:
            print(f"Warning: Could not emit final update: {emit_error}")
        
    except Exception as e:
        print(f"❌ Error during initial processing: {e}")
        try:
            socketio.emit('processing_status', {
                'status': 'error',
                'message': f'Processing error: {str(e)}'
            }, broadcast=True)
        except:
            pass

def find_available_port(start_port=5001, max_attempts=10):
    """Find an available port starting from start_port"""
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('0.0.0.0', port))
                return port
        except OSError:
            continue
    return None

def cleanup_processes():
    """Kill any existing Python processes using our port"""
    try:
        if os.name == 'nt':  # Windows
            os.system('taskkill /F /IM python.exe 2>nul')
            os.system('netstat -ano | findstr :5001 | for /f "tokens=5" %a in (\'more\') do taskkill /PID %a /F 2>nul')
        else:  # Unix/Linux
            os.system('pkill -f "python.*app.py"')
            os.system('lsof -ti:5001 | xargs kill -9 2>/dev/null')
    except:
        pass

if __name__ == '__main__':
    print("🚀 Chat Analytics Dashboard")
    print("=" * 50)
    
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print("\n🛑 Shutting down gracefully...")
        scheduler.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Check if we have existing data
    existing_channels = []
    has_existing_data = False
    
    try:
        existing_channels = processor.db.get_channels()
        has_existing_data = len(existing_channels) > 0
        
        if has_existing_data:
            print(f"✅ Found existing data for {len(existing_channels)} channels")
            print("🎯 Starting server immediately - existing data will be available!")
            print("🔄 New data processing will happen in background...")
        else:
            print("📝 No existing data found - will process initial data in background")
            print("⏳ Dashboard will load data as it's processed...")
            
    except Exception as e:
        print(f"⚠️  Error checking existing data: {e}")
        print("🔄 Will attempt processing in background...")
    
    # Start the background scheduler
    scheduler.add_job(
        func=process_and_update,
        trigger=IntervalTrigger(seconds=60),  # Run every 60 seconds
        id='process_chats',
        name='Process chat logs and update analytics',
        replace_existing=True
    )
    scheduler.start()
    
    # Start initial processing in background thread
    def delayed_processing():
        """Start processing after server is up"""
        import time
        time.sleep(2)  # Give server time to start
        run_initial_processing()
    
    initial_thread = threading.Thread(target=delayed_processing, daemon=True)
    initial_thread.start()
    
    # Shut down the scheduler when exiting the app
    atexit.register(lambda: scheduler.shutdown())
    
    # Find an available port
    port = find_available_port(5001)
    if port is None:
        print("❌ Could not find available port. Cleaning up processes...")
        cleanup_processes()
        port = find_available_port(5001)
        if port is None:
            print("❌ Still cannot find available port. Exiting...")
            sys.exit(1)
    
    print(f"\n🌐 Starting Flask-SocketIO server...")
    print(f"📍 Dashboard will be available at: http://localhost:{port}")
    
    if has_existing_data:
        print("🎉 Dashboard accessible immediately with existing data!")
        print("🔄 Background processing will update with any new data...")
    else:
        print("⏳ Dashboard will populate as data is processed in background...")
    
    print("\n" + "=" * 50)
    
    # Run the Flask-SocketIO server
    try:
        socketio.run(app, debug=False, host='0.0.0.0', port=port)
    except OSError as e:
        if "Address already in use" in str(e) or "10048" in str(e):
            print(f"❌ Port {port} is still in use. Trying to clean up...")
            cleanup_processes()
            import time
            time.sleep(2)
            port = find_available_port(5001)
            if port:
                print(f"🔄 Retrying on port {port}...")
                socketio.run(app, debug=False, host='0.0.0.0', port=port)
            else:
                print("❌ Could not start server. Please manually kill any running instances.")
                sys.exit(1)
        else:
            raise e