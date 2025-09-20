# Custom Consumer: Engagement Insights Analyzer
# Author: Eric Meyer
# Class: Streaming Data

## Overview
This project includes a custom consumer (`consumer_meyer.py`) that analyzes message engagement patterns by processing streaming JSON data in real-time.

## What It Does
The **Engagement Insights Consumer** calculates and stores engagement scores for each message based on:
- **Sentiment score** (0.0 to 1.0)
- **Message length** (character count)

### Engagement Score Calculation
The consumer uses a proprietary algorithm that:
1. Takes the sentiment score as a base (0-100 scale)
2. Applies length modifiers:
   - **Optimal length** (20-60 characters): +20% boost
   - **Too short** (<15 characters): -20% penalty  
   - **Too long** (>80 characters): -10% penalty
   - **Normal length**: No modifier

### Why This Insight Is Valuable
Understanding message engagement helps identify:
- What content resonates most with audiences
- Optimal message length for maximum impact
- Patterns in high-performing content
- Author engagement effectiveness

## Data Storage
**Database**: SQLite (`data/meyer_engagement_insights.sqlite`)

**Schema**:
```sql
CREATE TABLE engagement_insights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    author TEXT,
    timestamp TEXT,
    category TEXT,
    sentiment REAL,
    message_length INTEGER,
    engagement_score REAL,
    engagement_level TEXT,  -- "High", "Medium", "Low", "Very Low"
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Running the Project

### 1. Start the Producer
```bash
# Navigate to project directory
cd buzzline-05-meyer

# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the producer (generates live data)
python producers/producer_case.py
```

### 2. Run the Custom Consumer
In a separate terminal:
```bash
# Navigate to project directory
cd buzzline-05-meyer

# Activate virtual environment
source venv/bin/activate

# Run the custom engagement insights consumer
python consumers/consumer_meyer.py
```

## Output Example
```
=== Engagement Insights Consumer ===
This consumer analyzes message engagement based on sentiment and length
Reading from: data/project_live.json
Storing insights in: data/yourname_engagement_insights.sqlite

Processing message from Alice: 'I just found Python! It was amazing.'
✓ Stored engagement insight: High (104.4) for message by Alice

Processing message from Bob: 'I just tried a recipe! It was boring.'
✓ Stored engagement insight: Low (38.4) for message by Bob
```

## JSON Attributes Used
- `author`: Message author identification
- `timestamp`: When the message was created
- `category`: Message category classification
- `sentiment`: Sentiment score (0.0-1.0)
- `message_length`: Character count of the message
- `message`: The actual message text (for uniqueness tracking)

## Technical Details
- **Data Source**: Reads from local file (`data/project_live.json`)
- **Processing**: One message at a time as they arrive
- **Duplicate Prevention**: Tracks processed messages to avoid reprocessing
- **Real-time**: Polls for new messages every 2 seconds
- **Error Handling**: Graceful handling of missing files and malformed JSON