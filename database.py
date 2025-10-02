import sqlite3
import json
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Tuple, Optional

class DatabaseConnectionManager:
    """Thread-safe database connection manager with connection pooling and timeout handling"""
    
    def __init__(self, db_path: str, timeout: float = 30.0):
        self.db_path = db_path
        self.timeout = timeout
        self._lock = threading.Lock()
    
    @contextmanager
    def get_connection(self, timeout: Optional[float] = None):
        """Get a database connection with proper timeout and error handling"""
        if timeout is None:
            timeout = self.timeout
            
        conn = None
        try:
            # Configure SQLite connection with proper settings
            conn = sqlite3.connect(
                self.db_path, 
                timeout=timeout,
                check_same_thread=False
            )
            
            # Enable WAL mode for better concurrency
            conn.execute("PRAGMA journal_mode=WAL")
            
            # Set busy timeout
            conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
            
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys=ON")
            
            yield conn
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                print(f"Database lock detected, retrying in 1 second...")
                time.sleep(1)
                # Try one more time with a fresh connection
                try:
                    if conn:
                        conn.close()
                    conn = sqlite3.connect(
                        self.db_path, 
                        timeout=timeout,
                        check_same_thread=False
                    )
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
                    conn.execute("PRAGMA foreign_keys=ON")
                    yield conn
                except Exception as retry_e:
                    print(f"Database retry failed: {retry_e}")
                    raise e
            else:
                raise e
        except Exception as e:
            print(f"Database connection error: {e}")
            raise e
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass

    @contextmanager 
    def transaction(self, timeout: Optional[float] = None):
        """Execute operations within a transaction"""
        with self.get_connection(timeout) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

class ChatDatabase:
    def __init__(self, db_path="chat_data.db"):
        self.db_path = db_path
        self.db_manager = DatabaseConnectionManager(db_path, timeout=30.0)
        self.init_database()
    
    def init_database(self):
        """Initialize the database with required tables"""
        with self.db_manager.get_connection() as conn:
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
        with self.db_manager.transaction() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO processed_files 
                (channel, filename, file_path, last_processed_line, file_size, last_modified, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (channel, filename, file_path, last_line, file_size, last_modified))
    
    def insert_chat_messages(self, messages: List[Dict]):
        """Insert multiple chat messages"""
        if not messages:
            return
            
        with self.db_manager.transaction() as conn:
            cursor = conn.cursor()
            
            cursor.executemany('''
                INSERT INTO chat_messages (channel, username, message, timestamp, log_date)
                VALUES (?, ?, ?, ?, ?)
            ''', [(msg['channel'], msg['username'], msg['message'], 
                   msg['timestamp'], msg['log_date']) for msg in messages])
    
    def get_user_chat_counts(self, channel: str, date_filter: Optional[str] = None) -> Dict[str, int]:
        """Get chat counts for users in a channel with optional date filtering"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if date_filter:
            query, params = self._build_date_filter_query(channel, date_filter)
            cursor.execute(f'''
                SELECT username, COUNT(*) as chat_count
                FROM chat_messages 
                WHERE {query}
                GROUP BY username
                ORDER BY chat_count DESC
            ''', params)
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
    
    def _build_date_filter_query(self, channel: str, date_filter: str) -> Tuple[str, Tuple]:
        """Build SQL query and parameters for enhanced date filtering"""
        if date_filter.startswith('include:'):
            # Include specific dates: "include:2024-01-01,2024-01-03,2024-01-05"
            dates = date_filter[8:].split(',')  # Remove 'include:' prefix
            placeholders = ','.join(['?' for _ in dates])
            query = f"channel = ? AND log_date IN ({placeholders})"
            params = (channel,) + tuple(dates)
            
        elif date_filter.startswith('exclude:'):
            # Exclude specific dates: "exclude:2024-01-02,2024-01-04"
            dates = date_filter[8:].split(',')  # Remove 'exclude:' prefix
            placeholders = ','.join(['?' for _ in dates])
            query = f"channel = ? AND log_date NOT IN ({placeholders})"
            params = (channel,) + tuple(dates)
            
        elif ':' in date_filter and not date_filter.startswith(('include:', 'exclude:')):
            # Date range: "2024-01-01:2024-01-31"
            start_date, end_date = date_filter.split(':')
            query = "channel = ? AND log_date BETWEEN ? AND ?"
            params = (channel, start_date, end_date)
            
        else:
            # Single date: "2024-01-01"
            query = "channel = ? AND log_date = ?"
            params = (channel, date_filter)
            
        return query, params

    def get_user_messages(self, channel: str, date_filter: Optional[str] = None) -> Dict[str, List[str]]:
        """Get all messages for users in a channel with optional date filtering"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if date_filter:
            query, params = self._build_date_filter_query(channel, date_filter)
            cursor.execute(f'''
                SELECT username, message
                FROM chat_messages 
                WHERE {query}
                ORDER BY username, timestamp
            ''', params)
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
            query, params = self._build_date_filter_query(channel, date_filter)
            cursor.execute(f'''
                SELECT username, timestamp
                FROM chat_messages 
                WHERE {query}
                ORDER BY username, timestamp
            ''', params)
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
        with self.db_manager.transaction() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO user_stats 
                (channel, username, chat_count, alt_likelihood, similar_users, last_updated)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (channel, username, chat_count, alt_likelihood, json.dumps(similar_users)))
    
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
    
    def get_user_messages_paginated(self, channel: str, username: str, date_filter: Optional[str] = None, 
                                   page: int = 1, limit: int = 100) -> List[Dict]:
        """Get paginated messages for a specific user with timestamps"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        offset = (page - 1) * limit
        
        # Build query with date filtering
        if date_filter:
            query, params = self._build_date_filter_query(channel, date_filter)
            # Add username filter to the query
            query = query.replace("channel = ?", "channel = ? AND username = ?")
            params = (params[0], username) + params[1:]
        else:
            query = "channel = ? AND username = ?"
            params = (channel, username)
        
        cursor.execute(f'''
            SELECT message, timestamp, log_date
            FROM chat_messages 
            WHERE {query}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        ''', params + (limit, offset))
        
        messages = []
        for message, timestamp, log_date in cursor.fetchall():
            messages.append({
                'message': message,
                'timestamp': timestamp,
                'log_date': log_date
            })
        
        conn.close()
        return messages
    
    def get_user_channels(self, username: str) -> List[Dict]:
        """Get all channels where a user has been active"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT channel, COUNT(*) as message_count, 
                   MIN(log_date) as first_message, 
                   MAX(log_date) as last_message,
                   MAX(timestamp) as last_activity
            FROM chat_messages 
            WHERE username = ?
            GROUP BY channel
            ORDER BY message_count DESC
        ''', (username,))
        
        channels = []
        for channel, count, first, last, last_activity in cursor.fetchall():
            channels.append({
                'channel': channel,
                'message_count': count,
                'first_message_date': first,
                'last_message_date': last,
                'last_activity': last_activity
            })
        
        conn.close()
        return channels
    
    def get_user_activity_timeline(self, channel: str, username: str, 
                                 date_filter: Optional[str] = None) -> List[Dict]:
        """Get user's daily activity timeline"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query with date filtering
        if date_filter:
            query, params = self._build_date_filter_query(channel, date_filter)
            # Add username filter to the query
            query = query.replace("channel = ?", "channel = ? AND username = ?")
            params = (params[0], username) + params[1:]
        else:
            query = "channel = ? AND username = ?"
            params = (channel, username)
        
        cursor.execute(f'''
            SELECT log_date, COUNT(*) as message_count,
                   MIN(timestamp) as first_message_time,
                   MAX(timestamp) as last_message_time,
                   COUNT(DISTINCT substr(timestamp, 12, 2)) as active_hours
            FROM chat_messages 
            WHERE {query}
            GROUP BY log_date
            ORDER BY log_date DESC
            LIMIT 30
        ''', params)
        
        timeline = []
        for log_date, count, first_time, last_time, active_hours in cursor.fetchall():
            timeline.append({
                'date': log_date,
                'message_count': count,
                'first_message_time': first_time,
                'last_message_time': last_time,
                'active_hours': active_hours
            })
        
        conn.close()
        return timeline
    
    def get_user_temporal_analysis(self, channel: str, username: str, 
                                 date_filter: Optional[str] = None) -> Dict:
        """Get detailed temporal analysis for a user"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query with date filtering
        if date_filter:
            query, params = self._build_date_filter_query(channel, date_filter)
            query = query.replace("channel = ?", "channel = ? AND username = ?")
            params = (params[0], username) + params[1:]
        else:
            query = "channel = ? AND username = ?"
            params = (channel, username)
        
        # Get hourly distribution
        cursor.execute(f'''
            SELECT substr(timestamp, 12, 2) as hour, COUNT(*) as count
            FROM chat_messages 
            WHERE {query}
            GROUP BY hour
            ORDER BY hour
        ''', params)
        
        hourly_data = dict(cursor.fetchall())
        
        # Get daily averages and patterns
        cursor.execute(f'''
            SELECT 
                log_date,
                COUNT(*) as daily_count,
                MIN(timestamp) as first_msg,
                MAX(timestamp) as last_msg,
                COUNT(DISTINCT substr(timestamp, 12, 2)) as active_hours
            FROM chat_messages 
            WHERE {query}
            GROUP BY log_date
            ORDER BY log_date
        ''', params)
        
        daily_data = []
        for row in cursor.fetchall():
            log_date, count, first_msg, last_msg, active_hours = row
            
            # Calculate session length
            first_time = datetime.fromisoformat(first_msg)
            last_time = datetime.fromisoformat(last_msg)
            session_length = (last_time - first_time).total_seconds() / 3600  # hours
            
            daily_data.append({
                'date': log_date,
                'message_count': count,
                'active_hours': active_hours,
                'session_length': session_length,
                'messages_per_hour': count / max(active_hours, 1)
            })
        
        # Calculate overall statistics
        if daily_data:
            total_messages = sum(d['message_count'] for d in daily_data)
            active_days = len(daily_data)
            avg_messages_per_day = total_messages / active_days
            avg_session_length = sum(d['session_length'] for d in daily_data) / active_days
            avg_active_hours = sum(d['active_hours'] for d in daily_data) / active_days
            
            # Calculate message frequency patterns
            messages_per_hour = [d['messages_per_hour'] for d in daily_data]
            avg_messages_per_hour = sum(messages_per_hour) / len(messages_per_hour)
            
            # Determine peak activity hour
            peak_hour = max(hourly_data, key=hourly_data.get) if hourly_data else '12'
            peak_hour_messages = hourly_data.get(peak_hour, 0)
            
            # Calculate consistency (low variance = consistent)
            if len(messages_per_hour) > 1:
                import statistics
                consistency_score = 1 / (1 + statistics.variance(messages_per_hour))
            else:
                consistency_score = 1.0
                
        else:
            total_messages = avg_messages_per_day = avg_session_length = 0
            avg_active_hours = avg_messages_per_hour = 0
            peak_hour = '12'
            peak_hour_messages = 0
            consistency_score = 0
            active_days = 0
        
        conn.close()
        
        return {
            'hourly_distribution': hourly_data,
            'daily_patterns': daily_data,
            'total_messages': total_messages,
            'active_days': active_days,
            'avg_messages_per_day': round(avg_messages_per_day, 1),
            'avg_session_length': round(avg_session_length, 2),
            'avg_active_hours_per_day': round(avg_active_hours, 1),
            'avg_messages_per_hour': round(avg_messages_per_hour, 1),
            'peak_activity_hour': int(peak_hour),
            'peak_hour_messages': peak_hour_messages,
            'consistency_score': round(consistency_score, 3),
            'activity_intensity': min(avg_messages_per_hour / 10, 1.0)  # 0-1 scale
        }
    
    def get_user_behavioral_insights(self, channel: str, username: str, 
                                   date_filter: Optional[str] = None) -> List[Dict]:
        """Generate behavioral insights for a user"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query with date filtering
        if date_filter:
            query, params = self._build_date_filter_query(channel, date_filter)
            query = query.replace("channel = ?", "channel = ? AND username = ?")
            params = (params[0], username) + params[1:]
        else:
            query = "channel = ? AND username = ?"
            params = (channel, username)
        
        insights = []
        
        # Get basic message statistics
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total_msgs,
                AVG(LENGTH(message)) as avg_msg_length,
                COUNT(DISTINCT log_date) as active_days,
                MIN(timestamp) as first_msg,
                MAX(timestamp) as last_msg
            FROM chat_messages 
            WHERE {query}
        ''', params)
        
        result = cursor.fetchone()
        if result and result[0] > 0:
            total_msgs, avg_msg_length, active_days, first_msg, last_msg = result
            
            # Calculate activity span
            first_date = datetime.fromisoformat(first_msg)
            last_date = datetime.fromisoformat(last_msg)
            total_span_days = (last_date - first_date).days + 1
            activity_ratio = active_days / total_span_days if total_span_days > 0 else 1
            
            # Message frequency insight
            if total_msgs > 1000:
                insights.append({
                    'icon': 'fas fa-fire',
                    'title': 'High Activity User',
                    'description': f'Sent {total_msgs:,} messages across {active_days} days',
                    'value': f'{total_msgs/active_days:.1f} msgs/day',
                    'type': 'positive'
                })
            elif total_msgs > 100:
                insights.append({
                    'icon': 'fas fa-chart-line',
                    'title': 'Regular Participant',
                    'description': f'Consistent activity with {total_msgs} messages',
                    'value': f'{total_msgs/active_days:.1f} msgs/day',
                    'type': 'neutral'
                })
            else:
                insights.append({
                    'icon': 'fas fa-eye',
                    'title': 'Occasional User',
                    'description': f'Limited activity with {total_msgs} messages',
                    'value': f'{total_msgs/active_days:.1f} msgs/day',
                    'type': 'info'
                })
            
            # Message length insight
            if avg_msg_length > 100:
                insights.append({
                    'icon': 'fas fa-align-left',
                    'title': 'Detailed Communicator',
                    'description': 'Tends to write longer, detailed messages',
                    'value': f'{avg_msg_length:.0f} chars avg',
                    'type': 'info'
                })
            elif avg_msg_length < 20:
                insights.append({
                    'icon': 'fas fa-bolt',
                    'title': 'Quick Responder',
                    'description': 'Prefers short, concise messages',
                    'value': f'{avg_msg_length:.0f} chars avg',
                    'type': 'info'
                })
            
            # Activity consistency insight
            if activity_ratio > 0.7:
                insights.append({
                    'icon': 'fas fa-calendar-check',
                    'title': 'Highly Consistent',
                    'description': f'Active on {activity_ratio*100:.0f}% of days in timespan',
                    'value': f'{active_days}/{total_span_days} days',
                    'type': 'positive'
                })
            elif activity_ratio < 0.3:
                insights.append({
                    'icon': 'fas fa-calendar-times',
                    'title': 'Sporadic Activity',
                    'description': f'Active on {activity_ratio*100:.0f}% of days in timespan',
                    'value': f'{active_days}/{total_span_days} days',
                    'type': 'warning'
                })
        
        # Check for burst patterns (many messages in short time)
        cursor.execute(f'''
            SELECT COUNT(*) as burst_count
            FROM (
                SELECT log_date, COUNT(*) as daily_count
                FROM chat_messages 
                WHERE {query}
                GROUP BY log_date
                HAVING daily_count > 50
            )
        ''', params)
        
        burst_days = cursor.fetchone()[0]
        if burst_days > 0:
            insights.append({
                'icon': 'fas fa-rocket',
                'title': 'Burst Communicator',
                'description': f'Had high-activity days with 50+ messages',
                'value': f'{burst_days} burst days',
                'type': 'warning'
            })
        
        conn.close()
        return insights
    
    def get_user_temporal_analysis(self, channel: str, username: str, 
                                 date_filter: Optional[str] = None) -> Dict:
        """Get detailed temporal analysis for a user"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query with date filtering
        if date_filter:
            query, params = self._build_date_filter_query(channel, date_filter)
            query = query.replace("channel = ?", "channel = ? AND username = ?")
            params = (params[0], username) + params[1:]
        else:
            query = "channel = ? AND username = ?"
            params = (channel, username)
        
        # Get hourly distribution
        cursor.execute(f'''
            SELECT substr(timestamp, 12, 2) as hour, COUNT(*) as count
            FROM chat_messages 
            WHERE {query}
            GROUP BY hour
            ORDER BY hour
        ''', params)
        
        hourly_data = dict(cursor.fetchall())
        
        # Get daily averages and patterns
        cursor.execute(f'''
            SELECT 
                log_date,
                COUNT(*) as daily_count,
                MIN(timestamp) as first_msg,
                MAX(timestamp) as last_msg,
                COUNT(DISTINCT substr(timestamp, 12, 2)) as active_hours
            FROM chat_messages 
            WHERE {query}
            GROUP BY log_date
            ORDER BY log_date
        ''', params)
        
        daily_data = []
        for row in cursor.fetchall():
            log_date, count, first_msg, last_msg, active_hours = row
            
            # Calculate session length
            first_time = datetime.fromisoformat(first_msg)
            last_time = datetime.fromisoformat(last_msg)
            session_length = (last_time - first_time).total_seconds() / 3600  # hours
            
            daily_data.append({
                'date': log_date,
                'message_count': count,
                'active_hours': active_hours,
                'session_length': session_length,
                'messages_per_hour': count / max(active_hours, 1)
            })
        
        # Calculate overall statistics
        if daily_data:
            total_messages = sum(d['message_count'] for d in daily_data)
            active_days = len(daily_data)
            avg_messages_per_day = total_messages / active_days
            avg_session_length = sum(d['session_length'] for d in daily_data) / active_days
            avg_active_hours = sum(d['active_hours'] for d in daily_data) / active_days
            
            # Calculate message frequency patterns
            messages_per_hour = [d['messages_per_hour'] for d in daily_data]
            avg_messages_per_hour = sum(messages_per_hour) / len(messages_per_hour)
            
            # Determine peak activity hour
            peak_hour = max(hourly_data, key=hourly_data.get) if hourly_data else '12'
            peak_hour_messages = hourly_data.get(peak_hour, 0)
            
            # Calculate consistency (low variance = consistent)
            if len(messages_per_hour) > 1:
                variance = sum((x - avg_messages_per_hour) ** 2 for x in messages_per_hour) / len(messages_per_hour)
                consistency_score = 1.0 / (1.0 + variance)  # Higher score = more consistent
            else:
                consistency_score = 1.0
                
            return {
                'total_messages': total_messages,
                'active_days': active_days,
                'avg_messages_per_day': round(avg_messages_per_day, 2),
                'avg_session_length_hours': round(avg_session_length, 2),
                'avg_active_hours_per_day': round(avg_active_hours, 2),
                'avg_messages_per_hour': round(avg_messages_per_hour, 2),
                'peak_activity_hour': peak_hour,
                'peak_hour_messages': peak_hour_messages,
                'consistency_score': round(consistency_score, 3),
                'hourly_distribution': hourly_data,
                'daily_data': daily_data[-7:]  # Last 7 days for charts
            }
        else:
            return {
                'total_messages': 0,
                'active_days': 0,
                'avg_messages_per_day': 0,
                'avg_session_length_hours': 0,
                'avg_active_hours_per_day': 0,
                'avg_messages_per_hour': 0,
                'peak_activity_hour': '12',
                'peak_hour_messages': 0,
                'consistency_score': 0,
                'hourly_distribution': {},
                'daily_data': []
            }
        
        conn.close()
    
    def get_user_behavioral_insights(self, channel: str, username: str, 
                                   date_filter: Optional[str] = None) -> Dict:
        """Get behavioral insights and patterns for a user"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query with date filtering
        if date_filter:
            query, params = self._build_date_filter_query(channel, date_filter)
            query = query.replace("channel = ?", "channel = ? AND username = ?")
            params = (params[0], username) + params[1:]
        else:
            query = "channel = ? AND username = ?"
            params = (channel, username)
        
        # Get message patterns
        cursor.execute(f'''
            SELECT message, timestamp, log_date
            FROM chat_messages 
            WHERE {query}
            ORDER BY timestamp
        ''', params)
        
        messages_data = cursor.fetchall()
        
        if not messages_data:
            return {
                'message_frequency': {'per_minute': 0, 'per_hour': 0, 'per_day': 0},
                'activity_patterns': [],
                'writing_style': {},
                'engagement_level': 'Unknown',
                'activity_consistency': 0,
                'burst_messaging': False,
                'peak_activity_times': []
            }
        
        # Analyze message frequency patterns
        messages = [msg[0] for msg in messages_data]
        timestamps = [datetime.fromisoformat(ts[1]) for ts in messages_data]
        
        # Calculate time-based frequencies
        if len(timestamps) > 1:
            total_time_span = (timestamps[-1] - timestamps[0]).total_seconds()
            total_minutes = max(total_time_span / 60, 1)
            total_hours = max(total_time_span / 3600, 1)
            total_days = max(total_time_span / (3600 * 24), 1)
            
            messages_per_minute = len(messages) / total_minutes
            messages_per_hour = len(messages) / total_hours
            messages_per_day = len(messages) / total_days
        else:
            messages_per_minute = messages_per_hour = messages_per_day = 0
        
        # Analyze writing style
        total_chars = sum(len(msg) for msg in messages)
        avg_message_length = total_chars / len(messages) if messages else 0
        
        question_count = sum(1 for msg in messages if '?' in msg)
        exclamation_count = sum(1 for msg in messages if '!' in msg)
        caps_messages = sum(1 for msg in messages if msg.isupper() and len(msg) > 3)
        
        question_frequency = question_count / len(messages) if messages else 0
        exclamation_frequency = exclamation_count / len(messages) if messages else 0
        caps_frequency = caps_messages / len(messages) if messages else 0
        
        # Determine engagement level
        if messages_per_hour > 10:
            engagement_level = 'Very High'
        elif messages_per_hour > 5:
            engagement_level = 'High'
        elif messages_per_hour > 1:
            engagement_level = 'Medium'
        elif messages_per_hour > 0.1:
            engagement_level = 'Low'
        else:
            engagement_level = 'Very Low'
        
        # Analyze burst messaging (multiple messages in short time)
        burst_threshold = 60  # seconds
        burst_count = 0
        for i in range(1, len(timestamps)):
            if (timestamps[i] - timestamps[i-1]).total_seconds() < burst_threshold:
                burst_count += 1
        
        burst_messaging = burst_count > len(timestamps) * 0.3  # More than 30% are bursts
        
        # Find peak activity times (group by hour)
        hourly_activity = {}
        for ts in timestamps:
            hour = ts.hour
            hourly_activity[hour] = hourly_activity.get(hour, 0) + 1
        
        peak_hours = sorted(hourly_activity.items(), key=lambda x: x[1], reverse=True)[:3]
        peak_activity_times = [f"{hour:02d}:00" for hour, count in peak_hours]
        
        # Calculate activity consistency
        daily_counts = {}
        for ts in timestamps:
            date_key = ts.date().isoformat()
            daily_counts[date_key] = daily_counts.get(date_key, 0) + 1
        
        if len(daily_counts) > 1:
            daily_values = list(daily_counts.values())
            avg_daily = sum(daily_values) / len(daily_values)
            variance = sum((x - avg_daily) ** 2 for x in daily_values) / len(daily_values)
            consistency = 1.0 / (1.0 + variance / avg_daily) if avg_daily > 0 else 0
        else:
            consistency = 1.0
        
        insights = {
            'message_frequency': {
                'per_minute': round(messages_per_minute, 3),
                'per_hour': round(messages_per_hour, 2),
                'per_day': round(messages_per_day, 2)
            },
            'writing_style': {
                'avg_message_length': round(avg_message_length, 1),
                'question_frequency': round(question_frequency, 3),
                'exclamation_frequency': round(exclamation_frequency, 3),
                'caps_frequency': round(caps_frequency, 3)
            },
            'engagement_level': engagement_level,
            'activity_consistency': round(consistency, 3),
            'burst_messaging': burst_messaging,
            'peak_activity_times': peak_activity_times,
            'activity_patterns': [
                {
                    'pattern': 'High Activity Periods',
                    'description': f"Most active during {', '.join(peak_activity_times)}",
                    'value': len(peak_hours)
                },
                {
                    'pattern': 'Message Frequency',
                    'description': f"{messages_per_hour:.1f} messages per hour on average",
                    'value': round(messages_per_hour, 2)
                },
                {
                    'pattern': 'Engagement Style',
                    'description': f"{engagement_level} engagement with {'burst' if burst_messaging else 'steady'} messaging",
                    'value': engagement_level
                }
            ]
        }
        
        conn.close()
        return insights