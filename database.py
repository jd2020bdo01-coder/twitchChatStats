import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class ChatDatabase:
    def __init__(self, db_path="chat_data.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Table for storing chat messages
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                username TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp DATETIME NOT NULL,
                log_date DATE NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Table for tracking processed files
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                last_processed_line INTEGER DEFAULT 0,
                file_size INTEGER DEFAULT 0,
                last_modified DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, filename)
            )
        ''')
        
        # Table for storing user statistics
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                username TEXT NOT NULL,
                chat_count INTEGER DEFAULT 0,
                alt_likelihood REAL DEFAULT 0.0,
                similar_users TEXT, -- JSON array of similar users
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, username)
            )
        ''')
        
        # Table for storing stylometry groups
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stylometry_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                group_id INTEGER NOT NULL,
                usernames TEXT NOT NULL, -- JSON array of usernames in group
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, group_id)
            )
        ''')
        
        # Table for tracking analytics processing status
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analytics_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                last_processed_date DATE,
                total_messages INTEGER DEFAULT 0,
                last_analytics_update DATETIME DEFAULT CURRENT_TIMESTAMP,
                analytics_version INTEGER DEFAULT 1,
                UNIQUE(channel)
            )
        ''')
        
        # Optimized word storage: separate words dictionary
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS words_dictionary (
                word_id INTEGER PRIMARY KEY AUTOINCREMENT,
                word_text TEXT NOT NULL UNIQUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # User-word frequency relationships (normalized)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_word_frequencies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                username TEXT NOT NULL,
                word_id INTEGER NOT NULL,
                frequency INTEGER DEFAULT 1,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, username, word_id),
                FOREIGN KEY (word_id) REFERENCES words_dictionary (word_id)
            )
        ''')
        
        # Legacy table for backward compatibility (deprecated)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_words (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                username TEXT NOT NULL,
                word TEXT NOT NULL,
                frequency INTEGER DEFAULT 1,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, username, word)
            )
        ''')
        
        # Table for storing user similarity scores
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_similarities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                user1 TEXT NOT NULL,
                user2 TEXT NOT NULL,
                word_similarity REAL DEFAULT 0.0,
                pattern_similarity REAL DEFAULT 0.0,
                temporal_similarity REAL DEFAULT 0.0,
                behavioral_similarity REAL DEFAULT 0.0,
                combined_similarity REAL DEFAULT 0.0,
                confidence_score REAL DEFAULT 0.0,
                common_words INTEGER DEFAULT 0,
                total_compared_words INTEGER DEFAULT 0,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, user1, user2)
            )
        ''')
        
        # Table for storing detailed user patterns
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                username TEXT NOT NULL,
                avg_message_length REAL DEFAULT 0.0,
                message_length_variance REAL DEFAULT 0.0,
                punctuation_ratio REAL DEFAULT 0.0,
                exclamation_ratio REAL DEFAULT 0.0,
                question_ratio REAL DEFAULT 0.0,
                caps_ratio REAL DEFAULT 0.0,
                all_caps_frequency REAL DEFAULT 0.0,
                emoji_frequency REAL DEFAULT 0.0,
                unique_emoji_count INTEGER DEFAULT 0,
                repeated_char_frequency REAL DEFAULT 0.0,
                typo_frequency REAL DEFAULT 0.0,
                avg_words_per_message REAL DEFAULT 0.0,
                question_frequency REAL DEFAULT 0.0,
                exclamation_frequency REAL DEFAULT 0.0,
                statement_frequency REAL DEFAULT 0.0,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, username)
            )
        ''')
        
        # Table for storing temporal patterns
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_temporal_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel TEXT NOT NULL,
                username TEXT NOT NULL,
                peak_hours TEXT, -- JSON array of most active hours
                avg_session_duration REAL DEFAULT 0.0,
                avg_message_interval REAL DEFAULT 0.0,
                burst_frequency REAL DEFAULT 0.0,
                timezone_consistency REAL DEFAULT 0.0,
                activity_variance REAL DEFAULT 0.0,
                total_sessions INTEGER DEFAULT 0,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(channel, username)
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat_channel_date ON chat_messages(channel, log_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat_username ON chat_messages(username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_stats_channel ON user_stats(channel)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_words_channel_user ON user_words(channel, username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_words_word ON user_words(word)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_words_dictionary_text ON words_dictionary(word_text)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_word_frequencies_channel_user ON user_word_frequencies(channel, username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_word_frequencies_word_id ON user_word_frequencies(word_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_similarities_channel ON user_similarities(channel)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_similarities_users ON user_similarities(channel, user1, user2)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_patterns_channel_user ON user_patterns(channel, username)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_temporal_channel_user ON user_temporal_patterns(channel, username)')
        
        conn.commit()
        conn.close()
    
    def get_processed_file_info(self, channel: str, filename: str) -> Optional[Tuple[int, int, str]]:
        """Get processing info for a file: (last_line, file_size, last_modified)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT last_processed_line, file_size, last_modified 
            FROM processed_files 
            WHERE channel = ? AND filename = ?
        ''', (channel, filename))
        
        result = cursor.fetchone()
        conn.close()
        return result
    
    def update_processed_file_info(self, channel: str, filename: str, file_path: str, 
                                 last_line: int, file_size: int, last_modified: str):
        """Update or insert file processing information"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO processed_files 
            (channel, filename, file_path, last_processed_line, file_size, last_modified, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (channel, filename, file_path, last_line, file_size, last_modified))
        
        conn.commit()
        conn.close()
    
    def insert_chat_messages(self, messages: List[Dict]):
        """Insert multiple chat messages"""
        if not messages:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.executemany('''
            INSERT INTO chat_messages (channel, username, message, timestamp, log_date)
            VALUES (?, ?, ?, ?, ?)
        ''', [(msg['channel'], msg['username'], msg['message'], 
               msg['timestamp'], msg['log_date']) for msg in messages])
        
        conn.commit()
        conn.close()
    
    def get_user_chat_counts(self, channel: str, date_filter: Optional[str] = None) -> Dict[str, int]:
        """Get chat counts for users in a channel with optional date filtering"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if date_filter:
            if ':' in date_filter:
                start_date, end_date = date_filter.split(':')
                cursor.execute('''
                    SELECT username, COUNT(*) as chat_count
                    FROM chat_messages 
                    WHERE channel = ? AND log_date BETWEEN ? AND ?
                    GROUP BY username
                    ORDER BY chat_count DESC
                ''', (channel, start_date, end_date))
            else:
                cursor.execute('''
                    SELECT username, COUNT(*) as chat_count
                    FROM chat_messages 
                    WHERE channel = ? AND log_date = ?
                    GROUP BY username
                    ORDER BY chat_count DESC
                ''', (channel, date_filter))
        else:
            cursor.execute('''
                SELECT username, COUNT(*) as chat_count
                FROM chat_messages 
                WHERE channel = ?
                GROUP BY username
                ORDER BY chat_count DESC
            ''', (channel,))
        
        result = dict(cursor.fetchall())
        conn.close()
        return result
    
    def get_user_messages(self, channel: str, date_filter: Optional[str] = None) -> Dict[str, List[str]]:
        """Get all messages for users in a channel with optional date filtering"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if date_filter:
            if ':' in date_filter:
                start_date, end_date = date_filter.split(':')
                cursor.execute('''
                    SELECT username, message
                    FROM chat_messages 
                    WHERE channel = ? AND log_date BETWEEN ? AND ?
                    ORDER BY username, timestamp
                ''', (channel, start_date, end_date))
            else:
                cursor.execute('''
                    SELECT username, message
                    FROM chat_messages 
                    WHERE channel = ? AND log_date = ?
                    ORDER BY username, timestamp
                ''', (channel, date_filter))
        else:
            cursor.execute('''
                SELECT username, message
                FROM chat_messages 
                WHERE channel = ?
                ORDER BY username, timestamp
            ''', (channel,))
        
        user_messages = {}
        for username, message in cursor.fetchall():
            if username not in user_messages:
                user_messages[username] = []
            user_messages[username].append(message)
        
        conn.close()
        return user_messages
    
    def get_user_timestamps(self, channel: str, date_filter: Optional[str] = None) -> Dict[str, List[str]]:
        """Get all message timestamps for users in a channel with optional date filtering"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if date_filter:
            if ':' in date_filter:
                start_date, end_date = date_filter.split(':')
                cursor.execute('''
                    SELECT username, timestamp
                    FROM chat_messages 
                    WHERE channel = ? AND log_date BETWEEN ? AND ?
                    ORDER BY username, timestamp
                ''', (channel, start_date, end_date))
            else:
                cursor.execute('''
                    SELECT username, timestamp
                    FROM chat_messages 
                    WHERE channel = ? AND log_date = ?
                    ORDER BY username, timestamp
                ''', (channel, date_filter))
        else:
            cursor.execute('''
                SELECT username, timestamp
                FROM chat_messages 
                WHERE channel = ?
                ORDER BY username, timestamp
            ''', (channel,))
        
        user_timestamps = {}
        for username, timestamp in cursor.fetchall():
            if username not in user_timestamps:
                user_timestamps[username] = []
            user_timestamps[username].append(timestamp)
        
        conn.close()
        return user_timestamps
    
    def update_user_stats(self, channel: str, username: str, chat_count: int, 
                         alt_likelihood: float, similar_users: List[str]):
        """Update user statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO user_stats 
            (channel, username, chat_count, alt_likelihood, similar_users, last_updated)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (channel, username, chat_count, alt_likelihood, json.dumps(similar_users)))
        
        conn.commit()
        conn.close()
    
    def get_user_stats(self, channel: str) -> List[Dict]:
        """Get all user statistics for a channel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT username, chat_count, alt_likelihood, similar_users, last_updated
            FROM user_stats 
            WHERE channel = ?
            ORDER BY chat_count DESC
        ''', (channel,))
        
        results = []
        for row in cursor.fetchall():
            username, chat_count, alt_likelihood, similar_users_json, last_updated = row
            similar_users = json.loads(similar_users_json) if similar_users_json else []
            results.append({
                'username': username,
                'chat_count': chat_count,
                'alt_likelihood': alt_likelihood,
                'similar_users': similar_users,
                'last_updated': last_updated
            })
        
        conn.close()
        return results
    
    def get_date_range(self, channel: str) -> Tuple[str, str]:
        """Get the date range of data for a channel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT MIN(log_date), MAX(log_date)
            FROM chat_messages 
            WHERE channel = ?
        ''', (channel,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] and result[1]:
            return result[0], result[1]
        return "Unknown", "Unknown"
    
    def get_channels(self) -> List[str]:
        """Get list of all channels with data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT DISTINCT channel FROM chat_messages ORDER BY channel')
        channels = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return channels
    
    def update_stylometry_groups(self, channel: str, groups: List[List[str]]):
        """Update stylometry groups for a channel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Clear existing groups for this channel
        cursor.execute('DELETE FROM stylometry_groups WHERE channel = ?', (channel,))
        
        # Insert new groups
        for group_id, group in enumerate(groups):
            cursor.execute('''
                INSERT INTO stylometry_groups (channel, group_id, usernames)
                VALUES (?, ?, ?)
            ''', (channel, group_id, json.dumps(group)))
        
        conn.commit()
        conn.close()
    
    def get_unique_user_count(self, channel: str) -> int:
        """Get the number of unique user groups (estimated actual users)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM stylometry_groups WHERE channel = ?', (channel,))
        result = cursor.fetchone()
        
        conn.close()
        return result[0] if result else 0
    
    def get_available_dates(self, channel: str) -> List[str]:
        """Get list of all available dates for a channel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT log_date
            FROM chat_messages 
            WHERE channel = ?
            ORDER BY log_date
        ''', (channel,))
        
        dates = [row[0] for row in cursor.fetchall()]
        conn.close()
        return dates
    
    def get_analytics_status(self, channel: str) -> Optional[Dict]:
        """Get analytics processing status for a channel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT last_processed_date, total_messages, last_analytics_update, analytics_version
            FROM analytics_status 
            WHERE channel = ?
        ''', (channel,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'last_processed_date': result[0],
                'total_messages': result[1],
                'last_analytics_update': result[2],
                'analytics_version': result[3]
            }
        return None
    
    def update_analytics_status(self, channel: str, last_processed_date: str, total_messages: int):
        """Update analytics processing status for a channel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO analytics_status 
            (channel, last_processed_date, total_messages, last_analytics_update, analytics_version)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, 1)
        ''', (channel, last_processed_date, total_messages))
        
        conn.commit()
        conn.close()
    
    def get_total_messages_count(self, channel: str) -> int:
        """Get total number of messages for a channel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM chat_messages WHERE channel = ?', (channel,))
        result = cursor.fetchone()
        
        conn.close()
        return result[0] if result else 0
    
    def update_user_words_optimized(self, channel: str, username: str, words: Dict[str, int]):
        """Optimized word storage using normalized tables"""
        if not words:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get existing word frequencies for this user
        cursor.execute('''
            SELECT wd.word_text, uwf.frequency, uwf.word_id
            FROM user_word_frequencies uwf
            JOIN words_dictionary wd ON uwf.word_id = wd.word_id
            WHERE uwf.channel = ? AND uwf.username = ?
        ''', (channel, username))
        
        existing_words = {word_text: {'frequency': freq, 'word_id': word_id} 
                         for word_text, freq, word_id in cursor.fetchall()}
        
        # Process each word
        words_to_insert = []
        words_to_update = []
        
        for word_text, new_frequency in words.items():
            # Get or create word in dictionary
            word_id = self._get_or_create_word_id(cursor, word_text)
            
            if word_text in existing_words:
                # Update existing frequency if changed
                if existing_words[word_text]['frequency'] != new_frequency:
                    words_to_update.append((new_frequency, channel, username, word_id))
            else:
                # New word for this user
                words_to_insert.append((channel, username, word_id, new_frequency))
        
        # Remove words that are no longer used
        current_word_texts = set(words.keys())
        words_to_remove = [existing_words[word_text]['word_id'] 
                          for word_text in existing_words 
                          if word_text not in current_word_texts]
        
        # Execute batch operations
        if words_to_insert:
            cursor.executemany('''
                INSERT INTO user_word_frequencies (channel, username, word_id, frequency, last_updated)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', words_to_insert)
        
        if words_to_update:
            cursor.executemany('''
                UPDATE user_word_frequencies 
                SET frequency = ?, last_updated = CURRENT_TIMESTAMP
                WHERE channel = ? AND username = ? AND word_id = ?
            ''', words_to_update)
        
        if words_to_remove:
            cursor.executemany('''
                DELETE FROM user_word_frequencies 
                WHERE channel = ? AND username = ? AND word_id = ?
            ''', [(channel, username, word_id) for word_id in words_to_remove])
        
        conn.commit()
        conn.close()
    
    def _get_or_create_word_id(self, cursor, word_text: str) -> int:
        """Get existing word_id or create new word in dictionary"""
        # Try to get existing word
        cursor.execute('SELECT word_id FROM words_dictionary WHERE word_text = ?', (word_text,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # Create new word
        cursor.execute('''
            INSERT INTO words_dictionary (word_text, created_at)
            VALUES (?, CURRENT_TIMESTAMP)
        ''', (word_text,))
        
        return cursor.lastrowid
    
    def update_user_words(self, channel: str, username: str, words: Dict[str, int]):
        """Legacy method - redirects to optimized version"""
        self.update_user_words_optimized(channel, username, words)
    
    def get_user_words(self, channel: str, username: str) -> Dict[str, int]:
        """Get word frequencies for a user (optimized version)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Try optimized table first
        cursor.execute('''
            SELECT wd.word_text, uwf.frequency
            FROM user_word_frequencies uwf
            JOIN words_dictionary wd ON uwf.word_id = wd.word_id
            WHERE uwf.channel = ? AND uwf.username = ?
        ''', (channel, username))
        
        words = dict(cursor.fetchall())
        
        # Fallback to legacy table if no data in optimized table
        if not words:
            cursor.execute('''
                SELECT word, frequency FROM user_words 
                WHERE channel = ? AND username = ?
            ''', (channel, username))
            words = dict(cursor.fetchall())
        
        conn.close()
        return words
    
    def get_all_user_words(self, channel: str) -> Dict[str, Dict[str, int]]:
        """Get word frequencies for all users in a channel (optimized version)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Try optimized tables first
        cursor.execute('''
            SELECT uwf.username, wd.word_text, uwf.frequency
            FROM user_word_frequencies uwf
            JOIN words_dictionary wd ON uwf.word_id = wd.word_id
            WHERE uwf.channel = ?
            ORDER BY uwf.username, uwf.frequency DESC
        ''', (channel,))
        
        user_words = {}
        for username, word, frequency in cursor.fetchall():
            if username not in user_words:
                user_words[username] = {}
            user_words[username][word] = frequency
        
        # Fallback to legacy table if no data in optimized tables
        if not user_words:
            cursor.execute('''
                SELECT username, word, frequency FROM user_words 
                WHERE channel = ?
                ORDER BY username, frequency DESC
            ''', (channel,))
            
            for username, word, frequency in cursor.fetchall():
                if username not in user_words:
                    user_words[username] = {}
                user_words[username][word] = frequency
        
        conn.close()
        return user_words
    
    def get_word_statistics(self, channel: str) -> Dict:
        """Get statistics about word storage efficiency"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Count unique words in dictionary
        cursor.execute('SELECT COUNT(*) FROM words_dictionary')
        total_unique_words = cursor.fetchone()[0]
        
        # Count total word-user relationships
        cursor.execute('SELECT COUNT(*) FROM user_word_frequencies WHERE channel = ?', (channel,))
        total_relationships = cursor.fetchone()[0]
        
        # Count users in channel
        cursor.execute('SELECT COUNT(DISTINCT username) FROM user_word_frequencies WHERE channel = ?', (channel,))
        total_users = cursor.fetchone()[0]
        
        # Legacy storage count for comparison
        cursor.execute('SELECT COUNT(*) FROM user_words WHERE channel = ?', (channel,))
        legacy_entries = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'unique_words_in_dictionary': total_unique_words,
            'user_word_relationships': total_relationships,
            'users_analyzed': total_users,
            'legacy_entries': legacy_entries,
            'storage_efficiency': f"{total_relationships}/{legacy_entries if legacy_entries > 0 else 'N/A'}"
        }
    
    def update_user_similarity(self, channel: str, user1: str, user2: str, 
                             word_sim: float, pattern_sim: float, temporal_sim: float, behavioral_sim: float,
                             combined_sim: float, confidence: float, common_words: int, total_compared_words: int):
        """Update comprehensive similarity scores between two users"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Ensure consistent ordering (user1 <= user2 alphabetically)
        if user1 > user2:
            user1, user2 = user2, user1
        
        cursor.execute('''
            INSERT OR REPLACE INTO user_similarities 
            (channel, user1, user2, word_similarity, pattern_similarity, temporal_similarity, 
             behavioral_similarity, combined_similarity, confidence_score, common_words, 
             total_compared_words, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (channel, user1, user2, word_sim, pattern_sim, temporal_sim, behavioral_sim,
              combined_sim, confidence, common_words, total_compared_words))
        
        conn.commit()
        conn.close()
    
    def update_user_patterns(self, channel: str, username: str, patterns: Dict):
        """Store detailed user writing patterns"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO user_patterns 
            (channel, username, avg_message_length, message_length_variance, punctuation_ratio,
             exclamation_ratio, question_ratio, caps_ratio, all_caps_frequency, emoji_frequency,
             unique_emoji_count, repeated_char_frequency, typo_frequency, avg_words_per_message,
             question_frequency, exclamation_frequency, statement_frequency, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (channel, username, patterns.get('avg_message_length', 0),
              patterns.get('message_length_variance', 0), patterns.get('punctuation_ratio', 0),
              patterns.get('exclamation_ratio', 0), patterns.get('question_ratio', 0),
              patterns.get('caps_ratio', 0), patterns.get('all_caps_frequency', 0),
              patterns.get('emoji_frequency', 0), patterns.get('unique_emoji_count', 0),
              patterns.get('repeated_char_frequency', 0), patterns.get('typo_frequency', 0),
              patterns.get('avg_words_per_message', 0), patterns.get('question_frequency', 0),
              patterns.get('exclamation_frequency', 0), patterns.get('statement_frequency', 0)))
        
        conn.commit()
        conn.close()
    
    def update_user_temporal_patterns(self, channel: str, username: str, temporal_data: Dict):
        """Store user temporal activity patterns"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO user_temporal_patterns 
            (channel, username, peak_hours, avg_session_duration, avg_message_interval,
             burst_frequency, timezone_consistency, activity_variance, total_sessions, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (channel, username, json.dumps(temporal_data.get('peak_hours', [])),
              temporal_data.get('avg_session_duration', 0), temporal_data.get('avg_message_interval', 0),
              temporal_data.get('burst_frequency', 0), temporal_data.get('timezone_consistency', 0),
              temporal_data.get('activity_variance', 0), temporal_data.get('total_sessions', 0)))
        
        conn.commit()
        conn.close()
    
    def get_user_patterns(self, channel: str, username: str = None) -> Dict:
        """Get user writing patterns"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if username:
            cursor.execute('''
                SELECT * FROM user_patterns 
                WHERE channel = ? AND username = ?
            ''', (channel, username))
            result = cursor.fetchone()
            if result:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, result))
        else:
            cursor.execute('''
                SELECT * FROM user_patterns 
                WHERE channel = ?
            ''', (channel,))
            results = {}
            columns = [description[0] for description in cursor.description]
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                results[row_dict['username']] = row_dict
            return results
        
        conn.close()
        return {}
    
    def get_user_temporal_patterns(self, channel: str, username: str = None) -> Dict:
        """Get user temporal patterns"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if username:
            cursor.execute('''
                SELECT * FROM user_temporal_patterns 
                WHERE channel = ? AND username = ?
            ''', (channel, username))
            result = cursor.fetchone()
            if result:
                columns = [description[0] for description in cursor.description]
                data = dict(zip(columns, result))
                if data['peak_hours']:
                    data['peak_hours'] = json.loads(data['peak_hours'])
                return data
        else:
            cursor.execute('''
                SELECT * FROM user_temporal_patterns 
                WHERE channel = ?
            ''', (channel,))
            results = {}
            columns = [description[0] for description in cursor.description]
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                if row_dict['peak_hours']:
                    row_dict['peak_hours'] = json.loads(row_dict['peak_hours'])
                results[row_dict['username']] = row_dict
            return results
        
        conn.close()
        return {}
    
    def get_user_similarities(self, channel: str, username: str) -> List[Dict]:
        """Get similarity scores for a user compared to all other users"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT user1, user2, similarity_score, common_words, total_compared_words
            FROM user_similarities 
            WHERE channel = ? AND (user1 = ? OR user2 = ?)
            ORDER BY similarity_score DESC
        ''', (channel, username, username))
        
        similarities = []
        for user1, user2, score, common, total in cursor.fetchall():
            other_user = user2 if user1 == username else user1
            similarities.append({
                'user': other_user,
                'similarity_score': score,
                'common_words': common,
                'total_compared_words': total
            })
        
        conn.close()
        return similarities
    
    def get_top_user_similarities(self, channel: str) -> Dict[str, List[Dict]]:
        """Get top 5 most similar users for each user in the channel"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT user1, user2, similarity_score, common_words, total_compared_words
            FROM user_similarities 
            WHERE channel = ?
            ORDER BY user1, similarity_score DESC
        ''', (channel,))
        
        user_similarities = {}
        for user1, user2, score, common, total in cursor.fetchall():
            # Add for user1
            if user1 not in user_similarities:
                user_similarities[user1] = []
            if len(user_similarities[user1]) < 5:
                user_similarities[user1].append({
                    'user': user2,
                    'similarity_score': score,
                    'common_words': common,
                    'total_compared_words': total
                })
            
            # Add for user2
            if user2 not in user_similarities:
                user_similarities[user2] = []
            if len(user_similarities[user2]) < 5:
                user_similarities[user2].append({
                    'user': user1,
                    'similarity_score': score,
                    'common_words': common,
                    'total_compared_words': total
                })
        
        conn.close()
        return user_similarities