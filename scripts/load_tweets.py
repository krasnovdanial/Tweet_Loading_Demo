import json
import logging
import sqlite3
from pathlib import Path

DB_PATH = Path("tweets.db")
TWEETS_FILE = Path("../data/three_minutes_tweets.json.txt")
BATCH_SIZE = 1000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tweets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    tweet_text TEXT,
    country_code TEXT,
    display_url TEXT,
    lang TEXT,
    created_at TEXT,
    location TEXT,
    tweet_sentiment INTEGER DEFAULT 0
);
"""

INSERT_SQL = """
INSERT INTO tweets (name, tweet_text, country_code, display_url, lang, created_at, location)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def create_table(conn: sqlite3.Connection) -> None:
    """Create table if it doesn't exist"""
    with conn:
        conn.execute(CREATE_TABLE_SQL)


def parse_tweet(line: str) -> tuple | None:
    """Parse a tweet line and return a tuple or None"""
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        logging.warning("Passed in an invalid JSON line")
        return None

    if "delete" in data:
        return None

    try:
        name = data["user"]["name"]
        tweet_text = data.get("text")
        lang = data.get("lang")
        created_at = data.get("created_at")
        location = data["user"].get("location")

        country_code = (
            data.get("place", {}).get("country_code") if data.get("place") else None
        )

        display_url = None
        if data.get("entities") and data["entities"].get("urls"):
            display_url = data["entities"]["urls"][0].get("display_url")

        return name, tweet_text, country_code, display_url, lang, created_at, location

    except KeyError as e:
        logging.error(f"Processing got an error: {e}")
        return None


def load_tweets() -> None:
    """Load tweets from a file into SQLite3"""
    if not TWEETS_FILE.exists():
        logging.error(f"File is not found: {TWEETS_FILE}")
        return

    with sqlite3.connect(DB_PATH) as conn:
        create_table(conn)
        cursor = conn.cursor()

        batch, inserted, skipped = [], 0, 0

        with open(TWEETS_FILE, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue

                row = parse_tweet(line)
                if row:
                    batch.append(row)
                    inserted += 1
                else:
                    skipped += 1

                if len(batch) >= BATCH_SIZE:
                    cursor.executemany(INSERT_SQL, batch)
                    conn.commit()
                    logging.info(f"Inserted {inserted} tweets")
                    batch = []

        if batch:
            cursor.executemany(INSERT_SQL, batch)
            conn.commit()

        logging.info("Loading is completed")
        logging.info(f"Total inserted: {inserted}")
        logging.info(f"Passed: {skipped}")


if __name__ == "__main__":
    load_tweets()
