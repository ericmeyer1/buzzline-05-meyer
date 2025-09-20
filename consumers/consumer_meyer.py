"""
consumer_meyer.py

Custom consumer that processes JSON messages and extracts insights.

This consumer reads messages one at a time and stores processed insights
about message engagement patterns based on sentiment and message length.

Example JSON message structure:
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Insight: Calculating engagement score based on sentiment and message length
to understand what makes messages more engaging.
"""

import json
import sqlite3
import pathlib
import time
from typing import Optional, Dict, Any

# Configuration
DATA_FILE = pathlib.Path('data/project_live.json')
DB_FILE = pathlib.Path('data/meyer_engagement_insights.sqlite')

def init_db(db_file: pathlib.Path) -> None:
    """Initialize the SQLite database with engagement insights table."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS engagement_insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            author TEXT,
            timestamp TEXT,
            category TEXT,
            sentiment REAL,
            message_length INTEGER,
            engagement_score REAL,
            engagement_level TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {db_file}")

def calculate_engagement_score(sentiment: float, message_length: int) -> tuple[float, str]:
    """
    Calculate engagement score based on sentiment and message length.
    
    Logic:
    - Higher sentiment = more engaging
    - Optimal message length (20-60 chars) gets bonus
    - Very short or very long messages get penalty
    
    Args:
        sentiment: Sentiment score (0.0 to 1.0)
        message_length: Length of message in characters
    
    Returns:
        tuple: (engagement_score, engagement_level)
    """
    # Base score from sentiment (0-100 scale)
    base_score = sentiment * 100
    
    # Length modifier
    if 20 <= message_length <= 60:
        length_modifier = 1.2  # Boost for optimal length
    elif message_length < 15:
        length_modifier = 0.8  # Penalty for too short
    elif message_length > 80:
        length_modifier = 0.9  # Penalty for too long
    else:
        length_modifier = 1.0  # Neutral
    
    engagement_score = round(base_score * length_modifier, 2)
    
    # Categorize engagement level
    if engagement_score >= 80:
        engagement_level = "High"
    elif engagement_score >= 60:
        engagement_level = "Medium"
    elif engagement_score >= 40:
        engagement_level = "Low"
    else:
        engagement_level = "Very Low"
    
    return engagement_score, engagement_level

def store_engagement_insight(message: Dict[str, Any], db_file: pathlib.Path) -> None:
    """Store the processed engagement insight in the database."""
    # Extract required fields
    sentiment = message.get("sentiment", 0.0)
    message_length = message.get("message_length", 0)
    
    # Calculate engagement metrics
    engagement_score, engagement_level = calculate_engagement_score(sentiment, message_length)
    
    # Store in database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO engagement_insights 
        (author, timestamp, category, sentiment, message_length, engagement_score, engagement_level)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        message.get("author"),
        message.get("timestamp"),
        message.get("category"),
        sentiment,
        message_length,
        engagement_score,
        engagement_level
    ))
    
    conn.commit()
    conn.close()
    
    print(f"Stored engagement insight: {engagement_level} ({engagement_score}) for message by {message.get('author')}")

def read_one_message(data_file: pathlib.Path) -> Optional[Dict[str, Any]]:
    """
    Read one message from the live data file.
    
    This function reads the file and returns the first available message.
    In a real streaming scenario, this would be called repeatedly.
    """
    try:
        if not data_file.exists():
            print(f"Data file {data_file} does not exist yet.")
            return None
            
        with open(data_file, 'r') as f:
            lines = f.readlines()
            if lines:
                # Get the last line (most recent message)
                last_line = lines[-1].strip()
                if last_line:
                    message = json.loads(last_line)
                    return message
            return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

def main():
    """Main consumer loop that processes messages one at a time."""
    print("=== Engagement Insights Consumer ===")
    print("This consumer analyzes message engagement based on sentiment and length")
    print(f"Reading from: {DATA_FILE}")
    print(f"Storing insights in: {DB_FILE}")
    print("Press Ctrl+C to stop\n")
    
    # Initialize database
    init_db(DB_FILE)
    
    processed_messages = set()  # Track processed messages to avoid duplicates
    
    try:
        while True:
            message = read_one_message(DATA_FILE)
            
            if message:
                # Create a unique identifier for the message
                msg_id = f"{message.get('author')}_{message.get('timestamp')}_{message.get('message')}"
                
                if msg_id not in processed_messages:
                    print(f"Processing message from {message.get('author')}: '{message.get('message')[:50]}...'")
                    
                    # Process and store the insight
                    store_engagement_insight(message, DB_FILE)
                    processed_messages.add(msg_id)
                    
                    print("✓ Message processed successfully!\n")
                else:
                    print("Message already processed, skipping...")
            else:
                print("No new messages available, waiting...")
            
            # Wait before checking for new messages
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    except Exception as e:
        print(f"Error in consumer: {e}")
    finally:
        print("Consumer shutting down...")

if __name__ == "__main__":
    main()