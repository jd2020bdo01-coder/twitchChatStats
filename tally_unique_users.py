import os
import re
from collections import defaultdict
from datetime import datetime, timedelta

CHANNELS_ROOT = "Channels"
TIME_WINDOW = timedelta(seconds=2)  # Overlap window

def collect_user_times(channel_path):
    user_times = defaultdict(list)
    for log_file in os.listdir(channel_path):
        if log_file.endswith('.log'):
            with open(os.path.join(channel_path, log_file), encoding='utf-8') as f:
                for line in f:
                    match = re.match(r'^\[(\d{2}:\d{2}:\d{2})\] ([^:]+):', line)
                    if match:
                        time_str = match.group(1)
                        user = match.group(2).strip()
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', log_file)
                        if date_match:
                            dt_str = f"{date_match.group(1)} {time_str}"
                            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                            user_times[user].append(dt)
    return user_times

def has_overlap(times1, times2, threshold=TIME_WINDOW):
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

def group_unique_users(user_times):
    users = list(user_times.keys())
    n = len(users)
    groups = []
    assigned = set()
    for i in range(n):
        if users[i] in assigned:
            continue
        group = [users[i]]
        for j in range(i+1, n):
            if users[j] not in assigned and not has_overlap(user_times[users[i]], user_times[users[j]]):
                group.append(users[j])
                assigned.add(users[j])
        assigned.add(users[i])
        groups.append(group)
    return groups

def main():
    channels_root = "Channels"
    for channel in os.listdir(channels_root):
        channel_path = os.path.join(channels_root, channel)
        if os.path.isdir(channel_path):
            print(f"\n=== Channel: {channel} ===")
            user_times = collect_user_times(channel_path)
            groups = group_unique_users(user_times)
            print(f"Total unique user groups (possible unique people): {len(groups)}")
            for idx, group in enumerate(groups, 1):
                print(f"Group {idx}: {group}")

if __name__ == "__main__":
    main()
