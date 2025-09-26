#!/usr/bin/env python3
"""
Database Initialization Script
This script initializes the database and performs initial data processing.
"""

import os
import sys
from datetime import datetime
from database import ChatDatabase
from chat_processor import ChatProcessor

def main():
    print("Chat Analytics Database Initialization")
    print("=" * 50)
    
    # Initialize database
    print("Initializing database...")
    db = ChatDatabase()
    print("✓ Database initialized")
    
    # Initialize processor
    processor = ChatProcessor()
    
    # Check for existing data
    channels = db.get_channels()
    if channels:
        print(f"Found existing data for channels: {', '.join(channels)}")
        response = input("Reset database and reprocess all data? (y/N): ")
        if response.lower() == 'y':
            # Delete database file to reset
            if os.path.exists("chat_data.db"):
                os.remove("chat_data.db")
                print("✓ Database reset")
                # Reinitialize
                db = ChatDatabase()
                processor = ChatProcessor()
    
    # Process all channels
    print("\nProcessing chat logs...")
    results = processor.process_all_channels()
    
    total_messages = 0
    total_files = 0
    
    for channel, (messages, files) in results.items():
        total_messages += messages
        total_files += files
        if messages > 0:
            print(f"  {channel}: {messages:,} messages from {files} files")
    
    print(f"\nTotal: {total_messages:,} messages processed from {total_files} files")
    
    if total_messages > 0:
        # Update analytics for all channels
        print("\nUpdating analytics...")
        channels = db.get_channels()
        for channel in channels:
            print(f"  Analyzing {channel}...")
            processor.update_user_analytics(channel)
        
        print("✓ Analytics updated")
        
        # Display summary
        print("\nChannel Summary:")
        print("-" * 30)
        for channel in channels:
            summary = processor.get_channel_summary(channel)
            print(f"{channel}:")
            print(f"  Users: {summary['total_users']}")
            print(f"  Estimated Unique: {summary['unique_user_count']}")
            print(f"  Messages: {summary['total_messages']:,}")
            print(f"  Date Range: {summary['start_date']} to {summary['end_date']}")
            print()
    
    print("Initialization complete!")
    print("Run 'python run_server.py' to start the dashboard.")

if __name__ == "__main__":
    main()