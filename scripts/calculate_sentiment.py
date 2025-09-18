import logging
import sqlite3
from pathlib import Path

DB_PATH = Path("tweets.db")
AFINN_FILE = Path("../data/AFINN-111.txt")
TABLE_NAME = "tweets_normalized"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


def load_afinn() -> dict:
    """Load AFINN-111.txt into dictionary."""
    afinn = {}
    try:
        with open(AFINN_FILE, "r", encoding="utf-8") as f:
            for line in f:
                word, score = line.strip().split("\t")
                afinn[word] = int(score)
        logging.info(f"Loaded {len(afinn)} words from AFINN")
    except FileNotFoundError:
        logging.error(f"A file was not found: {AFINN_FILE}")
        raise
    return afinn


def calculate_sentiment(text: str, afinn: dict) -> int:
    """Calculate sentiment of text."""
    if not text:
        return 0
    words = text.lower().split()
    return sum(afinn.get(w, 0) for w in words)


def update_sentiments(table_name: str = TABLE_NAME):
    """Update a column tweet_sentiment with new sentiment."""
    afinn = load_afinn()

    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(f"SELECT rowid, tweet_text FROM {table_name}")
        rows = c.fetchall()

        updates = []
        for i, (tweet_id, text) in enumerate(rows, start=1):
            sentiment = calculate_sentiment(text, afinn)
            updates.append((sentiment, tweet_id))

            if i % 10000 == 0:
                logging.info(f"Processed {i} tweets")

        c.executemany(
            f"UPDATE {table_name} SET tweet_sentiment=? WHERE rowid=?", updates
        )
        conn.commit()

    logging.info(
        f"Sentiment is updated {len(updates)} tweets are located in {table_name}"
    )


if __name__ == "__main__":
    update_sentiments()
