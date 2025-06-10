-- Title: Entity Pair Sentiment Analysis
-- Description: This query analyzes the sentiment of news articles that mention both entities in a pair.
WITH entity_pairs AS (
    SELECT
        e1.entity_name as entity1,
        e2.entity_name as entity2,
        e1.source_uri,
        s.sentiment_score,
        s.sentiment_label
    FROM gold.entity_to_newsapi e1
    JOIN gold.entity_to_newsapi e2 ON e1.source_uri = e2.source_uri AND e1.entity_name < e2.entity_name
    JOIN gold.entity_to_newsapi_sentiment s ON e1.source_uri = s.uri
    WHERE ({{entity_type}} = 'All' OR e1.entity_type = {{entity_type}} OR e2.entity_type = {{entity_type}})
      AND ({{date_from}} IS NULL OR e1.source_published_at >= {{date_from}})
      AND ({{date_to}} IS NULL OR e1.source_published_at <= {{date_to}})
)
SELECT
    entity1,
    entity2,
    COUNT(*) as co_mentions,
    AVG(sentiment_score) as avg_sentiment_score,
    COUNT(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
    COUNT(CASE WHEN sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
    COUNT(CASE WHEN sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions,
    ROUND((COUNT(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as positive_percentage,
    ROUND((COUNT(CASE WHEN sentiment_label = 'NEUTRAL' THEN 1 END)::float / COUNT(*)) * 100, 2) as neutral_percentage,
    ROUND((COUNT(CASE WHEN sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*)) * 100, 2) as negative_percentage
FROM entity_pairs
GROUP BY entity1, entity2
HAVING COUNT(*) >= {{min_co_mentions}}
ORDER BY co_mentions DESC, avg_sentiment_score DESC
LIMIT 50;

-- Title: Entity Relationships by Sentiment
-- Description: This query categorizes entity relationships based on the dominant sentiment in their co-mentions.
WITH entity_pairs AS (
    SELECT
        e1.entity_name as entity1,
        e2.entity_name as entity2,
        e1.source_uri,
        s.sentiment_score,
        s.sentiment_label
    FROM gold.entity_to_newsapi e1
    JOIN gold.entity_to_newsapi e2 ON e1.source_uri = e2.source_uri AND e1.entity_name < e2.entity_name
    JOIN gold.entity_to_newsapi_sentiment s ON e1.source_uri = s.uri
    WHERE ({{entity_type}} = 'All' OR e1.entity_type = {{entity_type}} OR e2.entity_type = {{entity_type}})
      AND ({{date_from}} IS NULL OR e1.source_published_at >= {{date_from}})
      AND ({{date_to}} IS NULL OR e1.source_published_at <= {{date_to}})
),
sentiment_counts AS (
    SELECT
        entity1,
        entity2,
        COUNT(*) as co_mentions,
        COUNT(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
        COUNT(CASE WHEN sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
        COUNT(CASE WHEN sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions
    FROM entity_pairs
    GROUP BY entity1, entity2
    HAVING COUNT(*) >= {{min_co_mentions}}
)
SELECT
    entity1,
    entity2,
    co_mentions,
    CASE
        WHEN positive_mentions > neutral_mentions AND positive_mentions > negative_mentions THEN 'Positive'
        WHEN negative_mentions > neutral_mentions AND negative_mentions > positive_mentions THEN 'Negative'
        ELSE 'Neutral'
    END as dominant_sentiment,
    positive_mentions,
    neutral_mentions,
    negative_mentions,
    ROUND((positive_mentions::float / co_mentions) * 100, 2) as positive_percentage,
    ROUND((neutral_mentions::float / co_mentions) * 100, 2) as neutral_percentage,
    ROUND((negative_mentions::float / co_mentions) * 100, 2) as negative_percentage
FROM sentiment_counts
ORDER BY
    CASE
        WHEN dominant_sentiment = 'Positive' THEN 1
        WHEN dominant_sentiment = 'Neutral' THEN 2
        ELSE 3
    END,
    co_mentions DESC;

-- Title: Entity Sentiment Network
-- Description: This query provides data for visualizing a network of entities connected by sentiment.
WITH entity_pairs AS (
    SELECT
        e1.entity_name as entity1,
        e1.entity_type as entity1_type,
        e2.entity_name as entity2,
        e2.entity_type as entity2_type,
        e1.source_uri,
        s.sentiment_score,
        s.sentiment_label
    FROM gold.entity_to_newsapi e1
    JOIN gold.entity_to_newsapi e2 ON e1.source_uri = e2.source_uri AND e1.entity_name < e2.entity_name
    JOIN gold.entity_to_newsapi_sentiment s ON e1.source_uri = s.uri
    WHERE ({{entity_type}} = 'All' OR e1.entity_type = {{entity_type}} OR e2.entity_type = {{entity_type}})
      AND ({{date_from}} IS NULL OR e1.source_published_at >= {{date_from}})
      AND ({{date_to}} IS NULL OR e1.source_published_at <= {{date_to}})
),
sentiment_summary AS (
    SELECT
        entity1,
        entity1_type,
        entity2,
        entity2_type,
        COUNT(*) as co_mentions,
        AVG(sentiment_score) as avg_sentiment_score,
        COUNT(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 END) as positive_mentions,
        COUNT(CASE WHEN sentiment_label = 'NEUTRAL' THEN 1 END) as neutral_mentions,
        COUNT(CASE WHEN sentiment_label = 'NEGATIVE' THEN 1 END) as negative_mentions
    FROM entity_pairs
    GROUP BY entity1, entity1_type, entity2, entity2_type
    HAVING COUNT(*) >= {{min_co_mentions}}
)
SELECT
    entity1 as source,
    entity1_type as source_type,
    entity2 as target,
    entity2_type as target_type,
    co_mentions,
    avg_sentiment_score,
    CASE
        WHEN positive_mentions > neutral_mentions AND positive_mentions > negative_mentions THEN 'Positive'
        WHEN negative_mentions > neutral_mentions AND negative_mentions > positive_mentions THEN 'Negative'
        ELSE 'Neutral'
    END as dominant_sentiment
FROM sentiment_summary
ORDER BY co_mentions DESC, avg_sentiment_score DESC;

-- Title: Entity Sentiment Correlation
-- Description: This query identifies entities that have similar sentiment patterns across news sources.
WITH entity_sentiment AS (
    SELECT
        e.entity_name,
        e.source_name,
        AVG(s.sentiment_score) as avg_sentiment_score
    FROM gold.entity_to_newsapi e
    JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
    WHERE ({{entity_type}} = 'All' OR e.entity_type = {{entity_type}})
      AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
      AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
    GROUP BY e.entity_name, e.source_name
),
entity_pairs AS (
    SELECT
        e1.entity_name as entity1,
        e2.entity_name as entity2,
        COUNT(*) as common_sources,
        CORR(e1.avg_sentiment_score, e2.avg_sentiment_score) as sentiment_correlation
    FROM entity_sentiment e1
    JOIN entity_sentiment e2
        ON e1.source_name = e2.source_name
        AND e1.entity_name < e2.entity_name
    GROUP BY entity1, entity2
    HAVING COUNT(*) >= {{min_common_sources}}
)
SELECT
    entity1,
    entity2,
    common_sources,
    sentiment_correlation,
    CASE
        WHEN sentiment_correlation >= 0.7 THEN 'Strong Positive'
        WHEN sentiment_correlation >= 0.3 THEN 'Moderate Positive'
        WHEN sentiment_correlation >= -0.3 THEN 'Weak/No Correlation'
        WHEN sentiment_correlation >= -0.7 THEN 'Moderate Negative'
        ELSE 'Strong Negative'
    END as correlation_strength
FROM entity_pairs
WHERE sentiment_correlation IS NOT NULL
ORDER BY ABS(sentiment_correlation) DESC, common_sources DESC
LIMIT 50;

-- Title: Entity Sentiment Contrast
-- Description: This query identifies entities that have the most contrasting sentiment across different news sources.
WITH entity_source_sentiment AS (
    SELECT
        e.entity_name,
        e.source_name,
        AVG(s.sentiment_score) as avg_sentiment_score
    FROM gold.entity_to_newsapi e
    JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
    WHERE ({{entity_type}} = 'All' OR e.entity_type = {{entity_type}})
      AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
      AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
    GROUP BY e.entity_name, e.source_name
),
entity_sentiment_stats AS (
    SELECT
        entity_name,
        COUNT(DISTINCT source_name) as source_count,
        AVG(avg_sentiment_score) as overall_avg_sentiment,
        STDDEV(avg_sentiment_score) as sentiment_stddev,
        MIN(avg_sentiment_score) as min_sentiment,
        MAX(avg_sentiment_score) as max_sentiment,
        MAX(avg_sentiment_score) - MIN(avg_sentiment_score) as sentiment_range
    FROM entity_source_sentiment
    GROUP BY entity_name
    HAVING COUNT(DISTINCT source_name) >= {{min_sources}}
)
SELECT
    entity_name,
    source_count,
    overall_avg_sentiment,
    sentiment_stddev,
    min_sentiment,
    max_sentiment,
    sentiment_range
FROM entity_sentiment_stats
ORDER BY sentiment_range DESC, sentiment_stddev DESC
LIMIT 20;

-- Title: Entity Sentiment Change Over Time
-- Description: This query tracks how the sentiment between entity pairs changes over time.
WITH entity_pairs_over_time AS (
    SELECT
        e1.entity_name as entity1,
        e2.entity_name as entity2,
        date_trunc('month', e1.source_published_at) as month,
        AVG(s.sentiment_score) as avg_sentiment_score,
        COUNT(*) as co_mentions
    FROM gold.entity_to_newsapi e1
    JOIN gold.entity_to_newsapi e2
        ON e1.source_uri = e2.source_uri
        AND e1.entity_name < e2.entity_name
    JOIN gold.entity_to_newsapi_sentiment s ON e1.source_uri = s.uri
    WHERE ({{entity_type}} = 'All' OR e1.entity_type = {{entity_type}} OR e2.entity_type = {{entity_type}})
      AND ({{date_from}} IS NULL OR e1.source_published_at >= {{date_from}})
      AND ({{date_to}} IS NULL OR e1.source_published_at <= {{date_to}})
    GROUP BY entity1, entity2, month
    HAVING COUNT(*) >= {{min_monthly_mentions}}
),
entity_pair_changes AS (
    SELECT
        entity1,
        entity2,
        month,
        avg_sentiment_score,
        co_mentions,
        LAG(avg_sentiment_score) OVER (PARTITION BY entity1, entity2 ORDER BY month) as prev_sentiment_score
    FROM entity_pairs_over_time
)
SELECT
    entity1,
    entity2,
    month,
    avg_sentiment_score,
    prev_sentiment_score,
    avg_sentiment_score - COALESCE(prev_sentiment_score, 0) as sentiment_change,
    co_mentions
FROM entity_pair_changes
WHERE prev_sentiment_score IS NOT NULL
ORDER BY ABS(avg_sentiment_score - prev_sentiment_score) DESC, month DESC, co_mentions DESC
LIMIT 100;

-- Title: Entity Sentiment Clusters
-- Description: This query groups entities into clusters based on their sentiment patterns.
WITH entity_sentiment AS (
    SELECT
        e.entity_name,
        e.entity_type,
        COUNT(*) as mention_count,
        AVG(s.sentiment_score) as avg_sentiment_score,
        COUNT(CASE WHEN s.sentiment_label = 'POSITIVE' THEN 1 END)::float / COUNT(*) as positive_ratio,
        COUNT(CASE WHEN s.sentiment_label = 'NEUTRAL' THEN 1 END)::float / COUNT(*) as neutral_ratio,
        COUNT(CASE WHEN s.sentiment_label = 'NEGATIVE' THEN 1 END)::float / COUNT(*) as negative_ratio
    FROM gold.entity_to_newsapi e
    JOIN gold.entity_to_newsapi_sentiment s ON e.source_uri = s.uri
    WHERE ({{entity_type}} = 'All' OR e.entity_type = {{entity_type}})
      AND ({{date_from}} IS NULL OR e.source_published_at >= {{date_from}})
      AND ({{date_to}} IS NULL OR e.source_published_at <= {{date_to}})
    GROUP BY e.entity_name, e.entity_type
    HAVING COUNT(*) >= {{min_mentions}}
)
SELECT
    entity_name,
    entity_type,
    mention_count,
    avg_sentiment_score,
    positive_ratio,
    neutral_ratio,
    negative_ratio,
    CASE
        WHEN avg_sentiment_score >= 0.5 AND positive_ratio >= 0.6 THEN 'Highly Positive'
        WHEN avg_sentiment_score >= 0.2 AND positive_ratio >= 0.4 THEN 'Moderately Positive'
        WHEN avg_sentiment_score <= -0.5 AND negative_ratio >= 0.6 THEN 'Highly Negative'
        WHEN avg_sentiment_score <= -0.2 AND negative_ratio >= 0.4 THEN 'Moderately Negative'
        WHEN neutral_ratio >= 0.6 THEN 'Predominantly Neutral'
        ELSE 'Mixed Sentiment'
    END as sentiment_cluster
FROM entity_sentiment
ORDER BY
    CASE
        WHEN sentiment_cluster = 'Highly Positive' THEN 1
        WHEN sentiment_cluster = 'Moderately Positive' THEN 2
        WHEN sentiment_cluster = 'Predominantly Neutral' THEN 3
        WHEN sentiment_cluster = 'Mixed Sentiment' THEN 4
        WHEN sentiment_cluster = 'Moderately Negative' THEN 5
        ELSE 6
    END,
    mention_count DESC;
