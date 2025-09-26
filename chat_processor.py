import re
import os
import statistics
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Optional
from database import ChatDatabase

class ChatProcessor:
    def __init__(self, db_path="chat_data.db"):
        self.db = ChatDatabase(db_path)
    
    def parse_chat_line(self, line: str, channel: str, log_date: str) -> Optional[Dict]:
        """Parse a single chat line and return message data"""
        # Skip comment lines
        if line.strip().startswith('#'):
            return None
            
        # Extract timestamp, username, and message
        match = re.match(r'^\[(\d{2}:\d{2}:\d{2})\] ([^:]+): (.*)$', line.strip())
        if not match:
            return None
        
        time_str, username, message = match.groups()
        username = username.strip()
        message = message.strip()
        
        # Create full timestamp
        timestamp_str = f"{log_date} {time_str}"
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
        
        return {
            'channel': channel,
            'username': username,
            'message': message,
            'timestamp': timestamp.isoformat(),
            'log_date': log_date
        }
    
    def process_log_file(self, file_path: str, channel: str) -> int:
        """Process a log file and return number of new messages processed"""
        filename = os.path.basename(file_path)
        
        # Extract date from filename
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        if not date_match:
            return 0
        log_date = date_match.group(1)
        
        # Get file info
        try:
            file_stat = os.stat(file_path)
            file_size = file_stat.st_size
            last_modified = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
        except OSError:
            return 0
        
        # Check if file has been processed
        file_info = self.db.get_processed_file_info(channel, filename)
        
        start_line = 0
        if file_info:
            last_line, last_size, last_mod = file_info
            # If file hasn't changed, skip processing
            if file_size == last_size and last_modified == last_mod:
                return 0
            # If file has grown, start from last processed line
            if file_size >= last_size:
                start_line = last_line
        
        # Read and process new lines
        messages = []
        current_line = 0
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if current_line >= start_line:
                        message_data = self.parse_chat_line(line, channel, log_date)
                        if message_data:
                            messages.append(message_data)
                    current_line += 1
        except (UnicodeDecodeError, IOError):
            return 0
        
        # Insert new messages
        if messages:
            self.db.insert_chat_messages(messages)
        
        # Update file processing info
        self.db.update_processed_file_info(
            channel, filename, file_path, current_line, file_size, last_modified
        )
        
        return len(messages)
    
    def process_channel(self, channel_path: str) -> Tuple[int, int]:
        """Process all log files in a channel directory"""
        if not os.path.isdir(channel_path):
            return 0, 0
        
        channel = os.path.basename(channel_path)
        total_messages = 0
        files_processed = 0
        
        for log_file in os.listdir(channel_path):
            if log_file.endswith('.log'):
                file_path = os.path.join(channel_path, log_file)
                new_messages = self.process_log_file(file_path, channel)
                if new_messages > 0:
                    total_messages += new_messages
                    files_processed += 1
        
        return total_messages, files_processed
    
    def process_all_channels(self, channels_dir: str = "Channels") -> Dict[str, Tuple[int, int]]:
        """Process all channels and return processing summary"""
        if not os.path.exists(channels_dir):
            return {}
        
        results = {}
        for channel in os.listdir(channels_dir):
            channel_path = os.path.join(channels_dir, channel)
            if os.path.isdir(channel_path):
                messages, files = self.process_channel(channel_path)
                results[channel] = (messages, files)
        
        return results
    
    def has_overlap(self, times1: List[datetime], times2: List[datetime], 
                   threshold: timedelta = timedelta(seconds=2)) -> bool:
        """Check if two users have overlapping chat times"""
        if not times1 or not times2:
            return False
            
        i, j = 0, 0
        times1 = sorted(times1)
        times2 = sorted(times2)
        
        while i < len(times1) and j < len(times2):
            if abs((times1[i] - times2[j]).total_seconds()) <= threshold.total_seconds():
                return True
            if times1[i] < times2[j]:
                i += 1
            else:
                j += 1
        return False
    
    def analyze_users_comprehensive(self, channel: str, user_messages: Dict[str, List[str]], 
                                   user_timestamps: Dict[str, List[str]] = None,
                                   similarity_threshold: float = 0.3) -> Tuple[List[List[str]], Dict[str, float], Dict[str, List[str]]]:
        """Comprehensive user analysis with advanced pattern detection"""
        users = list(user_messages.keys())
        print(f"    Analyzing {len(users)} users with comprehensive pattern detection...")
        
        if len(users) <= 1:
            return [[u] for u in users], {u: 0.0 for u in users}, {u: [] for u in users}
        
        # Step 1: Analyze writing patterns
        print(f"    Step 1/5: Analyzing writing patterns...")
        writing_patterns = self.analyze_writing_patterns(channel, user_messages)
        
        # Step 2: Analyze temporal patterns
        print(f"    Step 2/5: Analyzing temporal patterns...")
        temporal_patterns = {}
        if user_timestamps:
            temporal_patterns = self.analyze_temporal_patterns(channel, user_timestamps)
        
        # Step 3: Build word frequency tables
        print(f"    Step 3/5: Building word frequency tables...")
        user_word_counts = self.build_word_frequencies(channel, user_messages)
        
        # Step 4: Calculate comprehensive similarities
        print(f"    Step 4/5: Calculating comprehensive similarities...")
        similarity_results = self.calculate_comprehensive_similarities(
            channel, users, user_word_counts, writing_patterns, temporal_patterns
        )
        
        # Step 5: Group users and generate results
        print(f"    Step 5/5: Grouping users and generating results...")
        groups, alt_scores, similar_users = self.generate_final_results(
            users, similarity_results, similarity_threshold
        )
        
        print(f"    Analysis complete: {len(groups)} groups found")
        return groups, alt_scores, similar_users
    
    def analyze_writing_patterns(self, channel: str, user_messages: Dict[str, List[str]]) -> Dict[str, Dict]:
        """Analyze detailed writing patterns for each user"""
        patterns = {}
        
        for username, messages in user_messages.items():
            if len(messages) < 3:
                patterns[username] = None
                continue
                
            # Message length analysis
            lengths = [len(msg) for msg in messages]
            avg_length = statistics.mean(lengths)
            length_variance = statistics.variance(lengths) if len(lengths) > 1 else 0
            
            # Punctuation analysis
            all_text = ' '.join(messages)
            total_chars = len(all_text)
            exclamations = all_text.count('!')
            questions = all_text.count('?')
            periods = all_text.count('.')
            
            # Capitalization analysis
            letters = [c for c in all_text if c.isalpha()]
            caps_count = sum(1 for c in letters if c.isupper()) if letters else 0
            caps_ratio = caps_count / len(letters) if letters else 0
            
            # Count ALL CAPS messages
            all_caps_msgs = sum(1 for msg in messages 
                              if len([c for c in msg if c.isalpha()]) > 3 and 
                              all(c.isupper() for c in msg if c.isalpha()))
            
            # Emoji/emoticon analysis
            emoji_pattern = re.compile(r':\)|:\(|:D|:P|;D|<3|XD|lol|lmao|kappa|poggers|kekw|lul|pepega|5head', re.IGNORECASE)
            emojis = emoji_pattern.findall(all_text.lower())
            
            # Typing speed indicators
            repeated_chars = sum(len(re.findall(r'(.)\1{2,}', msg)) for msg in messages)
            
            # Sentence type analysis
            question_msgs = sum(1 for msg in messages if '?' in msg)
            exclamation_msgs = sum(1 for msg in messages if '!' in msg)
            statement_msgs = len(messages) - question_msgs - exclamation_msgs
            
            # Word analysis
            all_words = ' '.join(messages).split()
            avg_words_per_msg = len(all_words) / len(messages) if messages else 0
            
            pattern_data = {
                'avg_message_length': avg_length,
                'message_length_variance': length_variance,
                'punctuation_ratio': (exclamations + questions + periods) / max(total_chars, 1),
                'exclamation_ratio': exclamations / max(total_chars, 1),
                'question_ratio': questions / max(total_chars, 1),
                'caps_ratio': caps_ratio,
                'all_caps_frequency': all_caps_msgs / len(messages),
                'emoji_frequency': len(emojis) / len(messages),
                'unique_emoji_count': len(set(emojis)),
                'repeated_char_frequency': repeated_chars / len(messages),
                'typo_frequency': 0,  # Could be enhanced with typo detection
                'avg_words_per_message': avg_words_per_msg,
                'question_frequency': question_msgs / len(messages),
                'exclamation_frequency': exclamation_msgs / len(messages),
                'statement_frequency': statement_msgs / len(messages)
            }
            
            patterns[username] = pattern_data
            
            # Store in database
            self.db.update_user_patterns(channel, username, pattern_data)
        
        return patterns
    
    def analyze_temporal_patterns(self, channel: str, user_timestamps: Dict[str, List[str]]) -> Dict[str, Dict]:
        """Analyze temporal activity patterns"""
        patterns = {}
        
        for username, timestamps in user_timestamps.items():
            if len(timestamps) < 5:
                patterns[username] = None
                continue
            
            # Convert timestamps to datetime objects
            try:
                times = []
                for ts in timestamps:
                    if isinstance(ts, str):
                        # Handle various timestamp formats
                        if 'T' in ts:
                            time_obj = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                        else:
                            time_obj = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                    else:
                        time_obj = ts
                    times.append(time_obj)
                
                times.sort()
                
                # Active hours analysis
                hours = [t.hour for t in times]
                hour_counts = Counter(hours)
                peak_hours = [hour for hour, count in hour_counts.most_common(3)]
                
                # Message intervals
                intervals = []
                for i in range(1, len(times)):
                    interval = (times[i] - times[i-1]).total_seconds()
                    if interval < 3600:  # Less than 1 hour
                        intervals.append(interval)
                
                avg_interval = statistics.mean(intervals) if intervals else 0
                
                # Burst detection (messages within 10 seconds)
                burst_count = sum(1 for i in range(1, min(len(times), 100)) 
                                if (times[i] - times[i-1]).total_seconds() < 10)
                
                # Session analysis (gaps > 30 minutes = new session)
                sessions = []
                current_session_start = times[0]
                current_session_msgs = 1
                
                for i in range(1, len(times)):
                    if (times[i] - times[i-1]).total_seconds() > 1800:  # 30 minutes
                        sessions.append({
                            'start': current_session_start,
                            'end': times[i-1],
                            'message_count': current_session_msgs
                        })
                        current_session_start = times[i]
                        current_session_msgs = 1
                    else:
                        current_session_msgs += 1
                
                # Add final session
                sessions.append({
                    'start': current_session_start,
                    'end': times[-1],
                    'message_count': current_session_msgs
                })
                
                avg_session_duration = statistics.mean([
                    (session['end'] - session['start']).total_seconds() 
                    for session in sessions
                ]) if sessions else 0
                
                temporal_data = {
                    'peak_hours': peak_hours,
                    'avg_session_duration': avg_session_duration,
                    'avg_message_interval': avg_interval,
                    'burst_frequency': burst_count / max(len(intervals), 1),
                    'timezone_consistency': 1.0,  # Could be enhanced
                    'activity_variance': statistics.variance(intervals) if len(intervals) > 1 else 0,
                    'total_sessions': len(sessions)
                }
                
                patterns[username] = temporal_data
                
                # Store in database
                self.db.update_user_temporal_patterns(channel, username, temporal_data)
                
            except (ValueError, TypeError) as e:
                print(f"    Warning: Could not parse timestamps for {username}: {e}")
                patterns[username] = None
        
        return patterns
    
    def build_word_frequencies(self, channel: str, user_messages: Dict[str, List[str]]) -> Dict[str, Dict[str, int]]:
        """Build word frequency tables for each user"""
        user_word_counts = {}
        
        for username, messages in user_messages.items():
            if len(messages) < 3:
                user_word_counts[username] = {}
                continue
                
            # Combine all messages and extract words
            all_text = ' '.join(messages).lower()
            words = re.findall(r"\b[a-zA-Z']+\b", all_text)
            
            # Filter out very short words and count frequencies
            word_counts = Counter(word for word in words if len(word) > 2)
            filtered_counts = {word: count for word, count in word_counts.items() if count >= 1}
            user_word_counts[username] = filtered_counts
            
            # Store in database
            self.db.update_user_words(channel, username, filtered_counts)
        
        return user_word_counts
    
    def calculate_comprehensive_similarities(self, channel: str, users: List[str], 
                                           word_counts: Dict, writing_patterns: Dict, 
                                           temporal_patterns: Dict) -> Dict[str, Dict]:
        """Calculate comprehensive similarity scores between all user pairs"""
        similarity_results = {}
        
        for i, user1 in enumerate(users):
            for j, user2 in enumerate(users):
                if i >= j:
                    continue
                
                # Word similarity (Jaccard)
                words1 = word_counts.get(user1, {})
                words2 = word_counts.get(user2, {})
                word_sim = self.calculate_word_similarity(words1, words2)
                
                # Pattern similarity
                pattern1 = writing_patterns.get(user1)
                pattern2 = writing_patterns.get(user2)
                pattern_sim = self.calculate_pattern_similarity(pattern1, pattern2)
                
                # Temporal similarity
                temporal1 = temporal_patterns.get(user1)
                temporal2 = temporal_patterns.get(user2)
                temporal_sim = self.calculate_temporal_similarity(temporal1, temporal2)
                
                # Behavioral similarity (placeholder for future enhancement)
                behavioral_sim = 0.0
                
                # Combined score with weights
                combined_sim = (
                    word_sim * 0.30 +       # Word overlap: 30%
                    pattern_sim * 0.40 +    # Writing patterns: 40%
                    temporal_sim * 0.25 +   # Temporal patterns: 25%
                    behavioral_sim * 0.05   # Behavioral: 5%
                )
                
                # Confidence score based on data availability
                confidence = self.calculate_confidence(pattern1, pattern2, temporal1, temporal2)
                
                similarity_results[f"{user1}|{user2}"] = {
                    'word_similarity': word_sim,
                    'pattern_similarity': pattern_sim,
                    'temporal_similarity': temporal_sim,
                    'behavioral_similarity': behavioral_sim,
                    'combined_similarity': min(combined_sim, 1.0),
                    'confidence': confidence,
                    'common_words': len(set(words1.keys()) & set(words2.keys())),
                    'total_words': len(set(words1.keys()) | set(words2.keys()))
                }
                
                # Store in database
                self.db.update_user_similarity(
                    channel, user1, user2, word_sim, pattern_sim, temporal_sim, 
                    behavioral_sim, combined_sim, confidence,
                    len(set(words1.keys()) & set(words2.keys())),
                    len(set(words1.keys()) | set(words2.keys()))
                )
        
        return similarity_results
    
    def calculate_word_similarity(self, words1: Dict[str, int], words2: Dict[str, int]) -> float:
        """Calculate Jaccard similarity for word overlap"""
        if not words1 or not words2:
            return 0.0
        
        common_words = set(words1.keys()) & set(words2.keys())
        total_words = set(words1.keys()) | set(words2.keys())
        
        return len(common_words) / len(total_words) if total_words else 0.0
    
    def calculate_pattern_similarity(self, pattern1: Dict, pattern2: Dict) -> float:
        """Calculate similarity between writing patterns"""
        if not pattern1 or not pattern2:
            return 0.0
        
        similarities = []
        
        # Message length similarity
        length_diff = abs(pattern1['avg_message_length'] - pattern2['avg_message_length'])
        length_sim = max(0, 1 - (length_diff / 100))
        similarities.append(length_sim)
        
        # Punctuation similarity
        punct_diff = abs(pattern1['punctuation_ratio'] - pattern2['punctuation_ratio'])
        punct_sim = max(0, 1 - punct_diff)
        similarities.append(punct_sim)
        
        # Capitalization similarity
        caps_diff = abs(pattern1['caps_ratio'] - pattern2['caps_ratio'])
        caps_sim = max(0, 1 - caps_diff)
        similarities.append(caps_sim)
        
        # Emoji similarity
        emoji_diff = abs(pattern1['emoji_frequency'] - pattern2['emoji_frequency'])
        emoji_sim = max(0, 1 - emoji_diff)
        similarities.append(emoji_sim)
        
        # Sentence type similarity
        question_diff = abs(pattern1['question_frequency'] - pattern2['question_frequency'])
        question_sim = max(0, 1 - question_diff)
        similarities.append(question_sim)
        
        return statistics.mean(similarities)
    
    def calculate_temporal_similarity(self, temporal1: Dict, temporal2: Dict) -> float:
        """Calculate similarity between temporal patterns"""
        if not temporal1 or not temporal2:
            return 0.0
        
        similarities = []
        
        # Peak hours overlap
        hours1 = set(temporal1.get('peak_hours', []))
        hours2 = set(temporal2.get('peak_hours', []))
        if hours1 and hours2:
            hour_overlap = len(hours1 & hours2) / len(hours1 | hours2)
            similarities.append(hour_overlap)
        
        # Session duration similarity
        duration1 = temporal1.get('avg_session_duration', 0)
        duration2 = temporal2.get('avg_session_duration', 0)
        if duration1 > 0 and duration2 > 0:
            duration_diff = abs(duration1 - duration2) / max(duration1, duration2)
            duration_sim = max(0, 1 - duration_diff)
            similarities.append(duration_sim)
        
        # Message interval similarity
        interval1 = temporal1.get('avg_message_interval', 0)
        interval2 = temporal2.get('avg_message_interval', 0)
        if interval1 > 0 and interval2 > 0:
            interval_diff = abs(interval1 - interval2) / max(interval1, interval2)
            interval_sim = max(0, 1 - interval_diff)
            similarities.append(interval_sim)
        
        return statistics.mean(similarities) if similarities else 0.0
    
    def calculate_confidence(self, pattern1: Dict, pattern2: Dict, temporal1: Dict, temporal2: Dict) -> float:
        """Calculate confidence score based on data availability"""
        confidence_factors = []
        
        # Pattern data availability
        if pattern1 and pattern2:
            confidence_factors.append(0.4)
        
        # Temporal data availability
        if temporal1 and temporal2:
            confidence_factors.append(0.3)
        
        # Message count factor (more messages = higher confidence)
        if pattern1 and pattern2:
            msg_count_factor = min(1.0, (pattern1.get('avg_message_length', 0) + 
                                        pattern2.get('avg_message_length', 0)) / 100)
            confidence_factors.append(msg_count_factor * 0.3)
        
        return sum(confidence_factors) if confidence_factors else 0.1
    
    def generate_final_results(self, users: List[str], similarity_results: Dict, 
                             threshold: float) -> Tuple[List[List[str]], Dict[str, float], Dict[str, List[str]]]:
        """Generate final groupings and alt scores"""
        alt_scores = {}
        similar_users = {}
        
        # Initialize
        for username in users:
            alt_scores[username] = 0.0
            similar_users[username] = []
        
        # Process similarities
        for pair_key, sim_data in similarity_results.items():
            user1, user2 = pair_key.split('|')
            combined_sim = sim_data['combined_similarity']
            confidence = sim_data['confidence']
            
            # Adjust similarity based on confidence
            adjusted_sim = combined_sim * confidence
            
            # Update alt scores
            alt_scores[user1] = max(alt_scores[user1], adjusted_sim)
            alt_scores[user2] = max(alt_scores[user2], adjusted_sim)
            
            # Add to similar users if above threshold
            if adjusted_sim >= threshold:
                confidence_indicator = f"({round(adjusted_sim*100,1)}%, conf: {round(confidence*100,1)}%)"
                similar_users[user1].append(f"{user2} {confidence_indicator}")
                similar_users[user2].append(f"{user1} {confidence_indicator}")
        
        # Group users
        groups = []
        assigned = set()
        
        for username in users:
            if username in assigned:
                continue
                
            group = [username]
            assigned.add(username)
            
            # Find similar users for this group
            for similar_info in similar_users[username]:
                similar_user = similar_info.split(' (')[0]
                if similar_user not in assigned and similar_user in users:
                    group.append(similar_user)
                    assigned.add(similar_user)
            
            groups.append(group)
        
        return groups, alt_scores, similar_users
    
    
    
    def get_channel_summary(self, channel: str) -> Dict:
        """Get summary data for a channel"""
        user_stats = self.db.get_user_stats(channel)
        start_date, end_date = self.db.get_date_range(channel)
        unique_user_count = self.db.get_unique_user_count(channel)
        total_messages = sum(stat['chat_count'] for stat in user_stats)
        
        return {
            'channel': channel,
            'user_stats': user_stats,
            'start_date': start_date,
            'end_date': end_date,
            'unique_user_count': unique_user_count,
            'total_users': len(user_stats),
            'total_messages': total_messages,
            'last_updated': datetime.now().isoformat()
        }
    
    def get_all_channels_summary(self) -> List[Dict]:
        """Get summary data for all channels"""
        channels = self.db.get_channels()
        return [self.get_channel_summary(channel) for channel in channels]
    
    def needs_analytics_update(self, channel: str, new_messages_count: int) -> bool:
        """Check if analytics need to be updated for a channel"""
        # If there are new messages, definitely update
        if new_messages_count > 0:
            return True
        
        # Check analytics status from database
        status = self.db.get_analytics_status(channel)
        if not status:
            return True  # No analytics exist
        
        # Check if user stats exist
        user_stats = self.db.get_user_stats(channel)
        if not user_stats:
            return True
        
        # Check current total message count vs cached count
        current_total = self.db.get_total_messages_count(channel)
        if current_total != status['total_messages']:
            return True  # Message count changed
        
        # Check if analytics are older than 24 hours
        try:
            last_update = datetime.fromisoformat(status['last_analytics_update'])
            time_since_update = datetime.now() - last_update
            
            # Update if older than 24 hours
            if time_since_update > timedelta(hours=24):
                return True
        except (ValueError, TypeError):
            return True  # Can't parse date, update to be safe
        
        return False
    
    def update_user_analytics(self, channel: str, date_filter: Optional[str] = None):
        """Update user analytics (chat counts, alt likelihood, similar users)"""
        print(f"  Running stylometry analysis for {channel}...")
        
        # Get chat counts
        chat_counts = self.db.get_user_chat_counts(channel, date_filter)
        if not chat_counts:
            return
        
        # Get user messages and timestamps for comprehensive analysis
        user_messages = self.db.get_user_messages(channel, date_filter)
        user_timestamps = self.db.get_user_timestamps(channel, date_filter)
        
        # Perform comprehensive analysis
        groups, alt_scores, similar_users = self.analyze_users_comprehensive(
            channel, user_messages, user_timestamps
        )
        
        # Update stylometry groups
        self.db.update_stylometry_groups(channel, groups)
        
        # Update user statistics
        for username in chat_counts:
            chat_count = chat_counts[username]
            alt_likelihood = alt_scores.get(username, 0.0) * 100  # Convert to percentage
            similar_user_list = similar_users.get(username, [])
            
            self.db.update_user_stats(channel, username, chat_count, alt_likelihood, similar_user_list)
        
        # Update analytics status
        if not date_filter:  # Only update status for full channel analysis
            start_date, end_date = self.db.get_date_range(channel)
            total_messages = self.db.get_total_messages_count(channel)
            self.db.update_analytics_status(channel, end_date, total_messages)
            print(f"  Analytics updated for {channel} ({total_messages} total messages)")