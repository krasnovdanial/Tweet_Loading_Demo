import logging
import sqlite3

DB_PATH = "tweets.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


def create_normalized_tables(c):
    """Creating normalized tables"""
    c.executescript(
        """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        location TEXT
    );

    CREATE TABLE IF NOT EXISTS countries (
        country_code TEXT PRIMARY KEY,
        country_name TEXT
    );

    CREATE TABLE IF NOT EXISTS urls (
        url_id INTEGER PRIMARY KEY AUTOINCREMENT,
        display_url TEXT
    );

    CREATE TABLE IF NOT EXISTS tweets_normalized (
        tweet_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        country_code TEXT,
        url_id INTEGER,
        tweet_text TEXT,
        lang TEXT,
        created_at TEXT,
        tweet_sentiment INTEGER DEFAULT 0,
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(country_code) REFERENCES countries(country_code),
        FOREIGN KEY(url_id) REFERENCES urls(url_id)
    );
    """
    )


def extract_country_name(location: str) -> str | None:
    """Extract country name from location"""
    if not location:
        return None
    if "," in location:
        return location.split(",")[-1].strip()
    return None


def normalize():
    """Normalize tweets"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    create_normalized_tables(c)

    c.execute(
        """
        SELECT name, location, country_code, display_url, tweet_text, lang, created_at, tweet_sentiment 
        FROM tweets
    """
    )
    rows = c.fetchall()

    inserted = 0
    for row in rows:
        (
            name,
            location,
            country_code,
            display_url,
            tweet_text,
            lang,
            created_at,
            sentiment,
        ) = row

        c.execute(
            "SELECT user_id FROM users WHERE name=? AND location=?", (name, location)
        )
        user = c.fetchone()
        if user:
            user_id = user[0]
        else:
            c.execute(
                "INSERT INTO users (name, location) VALUES (?, ?)", (name, location)
            )
            user_id = c.lastrowid

        country_name = extract_country_name(location)
        if country_code:
            c.execute(
                """INSERT INTO countries (country_code, country_name)
                        VALUES (?, ?)
                        ON CONFLICT(country_code) DO UPDATE SET country_name=excluded.country_name
                        WHERE excluded.country_name IS NOT NULL;""",
                (country_code, country_name),
            )

        url_id = None
        if display_url:
            c.execute("SELECT url_id FROM urls WHERE display_url=?", (display_url,))
            url = c.fetchone()
            if url:
                url_id = url[0]
            else:
                c.execute("INSERT INTO urls (display_url) VALUES (?)", (display_url,))
                url_id = c.lastrowid

        c.execute(
            """
            INSERT INTO tweets_normalized 
            (user_id, country_code, url_id, tweet_text, lang, created_at, tweet_sentiment)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (user_id, country_code, url_id, tweet_text, lang, created_at, sentiment),
        )

        inserted += 1
        if inserted % 10000 == 0:
            logging.info(f"Processed {inserted} tweets...")

    conn.commit()
    conn.close()
    logging.info(f"Normalization is done, processed {inserted} tweets.")


if __name__ == "__main__":
    normalize()
