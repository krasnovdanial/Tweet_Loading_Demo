-- The happiest country
SELECT country_code, AVG(tweet_sentiment) AS avg_sentiment
FROM tweets
GROUP BY country_code
ORDER BY avg_sentiment DESC
LIMIT 1;

-- The unhappiest country
SELECT country_code, AVG(tweet_sentiment) AS avg_sentiment
FROM tweets
GROUP BY country_code
ORDER BY avg_sentiment ASC
LIMIT 1;

-- The happiest location
SELECT location, AVG(tweet_sentiment) AS avg_sentiment
FROM tweets
GROUP BY location
ORDER BY avg_sentiment DESC
LIMIT 1;

-- The unhappiest location
SELECT location, AVG(tweet_sentiment) AS avg_sentiment
FROM tweets
GROUP BY location
ORDER BY avg_sentiment ASC
LIMIT 1;

-- The happiest person
SELECT name, AVG(tweet_sentiment) AS avg_sentiment
FROM tweets
GROUP BY name
ORDER BY avg_sentiment DESC
LIMIT 1;

-- The unhappiest person
SELECT name, AVG(tweet_sentiment) AS avg_sentiment
FROM tweets
GROUP BY name
ORDER BY avg_sentiment ASC
LIMIT 1;


-- The happiest person tweets
SELECT *
FROM tweets
WHERE name = (SELECT name
              FROM tweets
              GROUP BY name
              ORDER BY AVG(tweet_sentiment) DESC
              LIMIT 1);
