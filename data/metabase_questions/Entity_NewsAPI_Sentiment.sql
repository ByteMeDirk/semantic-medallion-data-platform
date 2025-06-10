-- Title: Entity Sentiment Overview
-- Description: This query provides an overview of sentiment for a specific entity across all news sources.
SELECT
    e.entity_name,
    e.entity_type,
    COUNT(*) as total_mentions,
    AVG(s.sentiment_score) as avg_sentiment_score,
    COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as positive_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END)::float / COUNT(*)) * 100, 2) as neutral_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as negative_percentage
FROM gold.entity_to_newsapi e
JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
WHERE e.entity_name = {{entity_name}}
  AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
GROUP BY e.entity_name, e.entity_type;

-- Title: Entity Sentiment Over Time
-- Description: This query shows how sentiment for a specific entity changes over time.
SELECT
    date_trunc('week', e.source_published_at) as week,
    e.entity_name,
    COUNT(*) as mention_count,
    AVG(s.sentiment_score) as avg_sentiment_score,
    COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions
FROM gold.entity_to_newsapi e
JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
WHERE e.entity_name = {{entity_name}}
  AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
GROUP BY week, e.entity_name
ORDER BY week;

-- Title: Top Entities by Positive Sentiment
-- Description: This query ranks entities by the percentage of positive sentiment mentions.
SELECT
    e.entity_name,
    e.entity_type,
    COUNT(*) as total_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as positive_percentage
FROM gold.entity_to_newsapi e
JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
WHERE ({{entity_type}} = 'All' OR e.entity_type = {{entity_type}})
  AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
GROUP BY e.entity_name, e.entity_type
HAVING COUNT(*) >= {{min_mentions}}
ORDER BY positive_percentage DESC, total_mentions DESC
LIMIT 20;

-- Title: Top Entities by Negative Sentiment
-- Description: This query ranks entities by the percentage of negative sentiment mentions.
SELECT
    e.entity_name,
    e.entity_type,
    COUNT(*) as total_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as negative_percentage
FROM gold.entity_to_newsapi e
JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
WHERE ({{entity_type}} = 'All' OR e.entity_type = {{entity_type}})
  AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
GROUP BY e.entity_name, e.entity_type
HAVING COUNT(*) >= {{min_mentions}}
ORDER BY negative_percentage DESC, total_mentions DESC
LIMIT 20;

-- Title: News Source Sentiment Analysis
-- Description: This query analyzes sentiment distribution across different news sources.
SELECT
    e.source_name,
    COUNT(*) as total_articles,
    AVG(s.sentiment_score) as avg_sentiment_score,
    COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END) as positive_articles,
    COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_articles,
    COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END) as negative_articles,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as positive_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END)::float / COUNT(*)) * 100, 2) as neutral_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as negative_percentage
FROM gold.entity_to_newsapi e
JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
WHERE ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
GROUP BY e.source_name
HAVING COUNT(*) >= {{min_articles}}
ORDER BY total_articles DESC;

-- Title: Entity Sentiment Comparison
-- Description: This query compares sentiment between two entities.
WITH entity1_sentiment AS (
    SELECT
        'Entity 1' as entity_group,
        e.entity_name,
        COUNT(*) as total_mentions,
        AVG(s.sentiment_score) as avg_sentiment_score,
        COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
        COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
        COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions,
        ROUND((COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as positive_percentage,
        ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END)::float / COUNT(*)) * 100, 2) as neutral_percentage,
        ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as negative_percentage
    FROM gold.entity_to_newsapi e
    JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
    WHERE e.entity_name = {{entity_name_1}}
      AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
      AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
    GROUP BY e.entity_name
),
entity2_sentiment AS (
    SELECT
        'Entity 2' as entity_group,
        e.entity_name,
        COUNT(*) as total_mentions,
        AVG(s.sentiment_score) as avg_sentiment_score,
        COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
        COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
        COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions,
        ROUND((COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as positive_percentage,
        ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END)::float / COUNT(*)) * 100, 2) as neutral_percentage,
        ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as negative_percentage
    FROM gold.entity_to_newsapi e
    JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
    WHERE e.entity_name = {{entity_name_2}}
      AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
      AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
    GROUP BY e.entity_name
)
SELECT * FROM entity1_sentiment
UNION ALL
SELECT * FROM entity2_sentiment;

-- Title: Entity Sentiment by News Source
-- Description: This query shows sentiment distribution for a specific entity across different news sources.
SELECT
    e.source_name,
    COUNT(*) as total_mentions,
    AVG(s.sentiment_score) as avg_sentiment_score,
    COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as positive_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END)::float / COUNT(*)) * 100, 2) as neutral_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as negative_percentage
FROM gold.entity_to_newsapi e
JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
WHERE e.entity_name = {{entity_name}}
  AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
GROUP BY e.source_name
HAVING COUNT(*) >= {{min_mentions}}
ORDER BY total_mentions DESC;

-- Title: Entity Type Sentiment Analysis
-- Description: This query analyzes sentiment distribution across different entity types.
SELECT
    e.entity_type,
    COUNT(*) as total_mentions,
    COUNT(DISTINCT e.entity_name) as unique_entities,
    AVG(s.sentiment_score) as avg_sentiment_score,
    COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as positive_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END)::float / COUNT(*)) * 100, 2) as neutral_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as negative_percentage
FROM gold.entity_to_newsapi e
JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
WHERE ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
GROUP BY e.entity_type
ORDER BY total_mentions DESC;

-- Title: Sentiment Trend Analysis
-- Description: This query shows overall sentiment trends over time across all entities.
SELECT
    date_trunc('month', e.source_published_at) as month,
    COUNT(*) as total_mentions,
    AVG(s.sentiment_score) as avg_sentiment_score,
    COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
    COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as positive_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END)::float / COUNT(*)) * 100, 2) as neutral_percentage,
    ROUND((COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as negative_percentage
FROM gold.entity_to_newsapi e
JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
WHERE ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
GROUP BY month
ORDER BY month;

-- Title: Entity Sentiment Articles
-- Description: This query lists individual articles with their sentiment for a specific entity.
SELECT
    e.entity_name,
    e.source_title,
    e.source_description,
    e.source_url,
    e.source_published_at,
    s.sentiment_score,
    s.sentiment_label,
    e.match_score
FROM gold.entity_to_newsapi e
JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
WHERE e.entity_name = {{entity_name}}
  AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
  AND ({{sentiment}} = 'All' OR s.sentiment_label = {{sentiment}})
ORDER BY e.source_published_at DESC, s.sentiment_score DESC;
