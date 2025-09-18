import json

TWEETS_FILE = "../data/three_minutes_tweets.json.txt"
OUTPUT_FILE = "tweet_keys.txt"


def collect_keys(max_lines=1000):
    keys = set()

    with open(TWEETS_FILE, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            def walk(d, prefix=""):
                if isinstance(d, dict):
                    for k, v in d.items():
                        new_key = f"{prefix}.{k}" if prefix else k
                        keys.add(new_key)
                        walk(v, new_key)
                elif isinstance(d, list):
                    if d and isinstance(d[0], dict):
                        # Проверяем только первую структуру списка
                        walk(d[0], f"{prefix}[]")

            walk(data)

            if i >= max_lines:
                break

    return sorted(keys)


if __name__ == "__main__":
    all_keys = collect_keys(max_lines=1000)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        out.write("\n".join(all_keys))

    print(f"✅ Собрано {len(all_keys)} уникальных ключей. Смотри файл {OUTPUT_FILE}")
