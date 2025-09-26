#!/usr/bin/env python3
"""
Chat Analytics Server Runner
This script provides a simple way to start the chat analytics server.
"""

import sys
import os
import subprocess
from pathlib import Path

def check_requirements():
    """Check if all required packages are installed"""
    try:
        import flask
        import flask_socketio
        import pandas
        import sklearn
        import apscheduler
        print("✓ All required packages are installed")
        return True
    except ImportError as e:
        print(f"✗ Missing required package: {e}")
        print("Please install requirements with: pip install -r requirements.txt")
        return False

def check_environment():
    """Check if the environment is properly set up"""
    # Check if Channels directory exists
    if not os.path.exists("Channels"):
        print("✗ Channels directory not found")
        print("Please ensure the 'Channels' directory with log files exists")
        return False
    
    # Check if there are any log files
    log_files_found = False
    for root, dirs, files in os.walk("Channels"):
        for file in files:
            if file.endswith('.log'):
                log_files_found = True
                break
        if log_files_found:
            break
    
    if not log_files_found:
        print("⚠ No .log files found in Channels directory")
        print("The server will start but no data will be available until log files are added")
    else:
        print("✓ Log files found in Channels directory")
    
    return True

def main():
    print("Chat Analytics Server")
    print("=" * 50)
    
    # Check requirements
    if not check_requirements():
        sys.exit(1)
    
    # Check environment
    if not check_environment():
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    print("\nStarting server...")
    print("Dashboard will be available at: http://localhost:5001")
    print("Press Ctrl+C to stop the server")
    print("-" * 50)
    
    try:
        # Import and run the Flask app
        from app import app, socketio
        socketio.run(app, debug=False, host='0.0.0.0', port=5001)
    except KeyboardInterrupt:
        print("\nServer stopped by user")
    except Exception as e:
        print(f"Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()