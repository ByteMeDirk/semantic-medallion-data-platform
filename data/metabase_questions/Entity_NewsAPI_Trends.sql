-- Title: Entity Mention Trends
-- Description: This query shows the trend of entity mentions over time with a moving average.
SELECT
    date_trunc('day', source_published_at) as day,
    entity_name,
    count(*) as daily_mentions,
    avg(count(*)) OVER (
        PARTITION BY entity_name
        ORDER BY date_trunc('day', source_published_at)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as seven_day_moving_avg
FROM gold.entity_to_newsapi
WHERE entity_name IN (
    SELECT entity_name
    FROM gold.entity_to_newsapi
    WHERE ({{entity_type}} = 'All' OR entity_type = {{entity_type}})
    GROUP BY entity_name
    ORDER BY count(*) DESC
    LIMIT {{top_n_entities}}
)
AND source_published_at IS NOT NULL
AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY day, entity_name
ORDER BY day, entity_name;

-- Title: Entity Type Popularity Over Time
-- Description: This query tracks the popularity of different entity types over time.
SELECT
    date_trunc('month', source_published_at) as month,
    entity_type,
    count(*) as mention_count,
    count(distinct entity_name) as unique_entities
FROM gold.entity_to_newsapi
WHERE source_published_at IS NOT NULL
AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY month, entity_type
ORDER BY month, mention_count DESC;

-- Title: Entity Match Score Trends
-- Description: This query shows how match scores for entity affiliations have changed over time.
SELECT
    date_trunc('month', source_published_at) as month,
    avg(match_score) as avg_match_score,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY match_score) as median_match_score,
    min(match_score) as min_match_score,
    max(match_score) as max_match_score
FROM gold.entity_to_newsapi
WHERE source_published_at IS NOT NULL
AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY month
ORDER BY month;

-- Title: News Source Activity Trends
-- Description: This query shows the activity trends of news sources over time.
SELECT
    date_trunc('month', source_published_at) as month,
    source_name,
    count(*) as article_count,
    count(distinct entity_name) as entity_coverage
FROM gold.entity_to_newsapi
WHERE source_published_at IS NOT NULL
AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY month, source_name
ORDER BY month, article_count DESC;

-- Title: Entity Relationship Growth
-- Description: This query analyzes how entity relationships have grown over time based on news mentions.
WITH entity_first_mentions AS (
    SELECT
        entity_name,
        min(source_published_at) as first_mention_date
    FROM gold.entity_to_newsapi
    WHERE source_published_at IS NOT NULL
    GROUP BY entity_name
)
SELECT
    date_trunc('month', first_mention_date) as month,
    count(*) as new_entities,
    sum(count(*)) OVER (ORDER BY date_trunc('month', first_mention_date)) as cumulative_entities
FROM entity_first_mentions
WHERE ({{date_from}} IS NULL OR first_mention_date >= {{date_from}})
AND ({{date_to}} IS NULL OR first_mention_date <= {{date_to}})
GROUP BY month
ORDER BY month;

-- Title: Entity Pair Co-mention Trends
-- Description: This query tracks how frequently pairs of entities are co-mentioned over time.
WITH entity_mentions AS (
    SELECT
        date_trunc('month', source_published_at) as month,
        source_uri,
        entity_name
    FROM gold.entity_to_newsapi
    WHERE source_published_at IS NOT NULL
    AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
    AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
),
entity_pairs AS (
    SELECT
        a.month,
        a.entity_name as entity1,
        b.entity_name as entity2,
        count(distinct a.source_uri) as co_mention_count
    FROM entity_mentions a
    JOIN entity_mentions b
        ON a.source_uri = b.source_uri
        AND a.month = b.month
        AND a.entity_name < b.entity_name
    GROUP BY a.month, entity1, entity2
)
WITH ranked_entity_pairs AS (
    SELECT
        month,
        entity1,
        entity2,
        co_mention_count,
        rank() OVER (PARTITION BY month ORDER BY co_mention_count DESC) as rank
    FROM entity_pairs
    WHERE (entity1 = {{entity_name}} OR entity2 = {{entity_name}} OR {{entity_name}} = 'All')
)
SELECT
    month,
    entity1,
    entity2,
    co_mention_count,
    rank
FROM ranked_entity_pairs
WHERE rank <= 10
ORDER BY month, co_mention_count DESC;

-- Title: Entity Seasonal Patterns
-- Description: This query identifies seasonal patterns in entity mentions.
SELECT
    extract(month from source_published_at) as month_number,
    to_char(source_published_at, 'Month') as month_name,
    entity_name,
    count(*) as mention_count,
    avg(match_score) as avg_match_score
FROM gold.entity_to_newsapi
WHERE entity_name = {{entity_name}}
AND source_published_at IS NOT NULL
AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY month_number, month_name, entity_name
ORDER BY month_number;

-- Title: Entity Mention Velocity
-- Description: This query calculates the velocity (rate of change) of entity mentions over time.
WITH monthly_mentions AS (
    SELECT
        date_trunc('month', source_published_at) as month,
        entity_name,
        count(*) as mention_count
    FROM gold.entity_to_newsapi
    WHERE source_published_at IS NOT NULL
    AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
    AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
    GROUP BY month, entity_name
),
mention_velocity AS (
    SELECT
        current_month.month,
        current_month.entity_name,
        current_month.mention_count,
        lag(current_month.mention_count) OVER (
            PARTITION BY current_month.entity_name
            ORDER BY current_month.month
        ) as previous_month_count
    FROM monthly_mentions current_month
)
SELECT
    month,
    entity_name,
    mention_count,
    previous_month_count,
    mention_count - COALESCE(previous_month_count, 0) as absolute_change,
    CASE
        WHEN COALESCE(previous_month_count, 0) = 0 THEN NULL
        ELSE round(((mention_count - previous_month_count) / previous_month_count::float) * 100, 2)
    END as percentage_change
FROM mention_velocity
WHERE entity_name IN (
    SELECT entity_name
    FROM gold.entity_to_newsapi
    WHERE ({{entity_type}} = 'All' OR entity_type = {{entity_type}})
    GROUP BY entity_name
    ORDER BY count(*) DESC
    LIMIT {{top_n_entities}}
)
ORDER BY month, mention_count DESC;

-- Title: Entity Affiliation Strength Evolution
-- Description: This query tracks how the strength of entity affiliations evolves over time.
WITH entity_mentions AS (
    SELECT
        entity_name,
        affiliated_entity_name,
        date_trunc('quarter', source_published_at) as quarter,
        avg(match_score) as avg_match_score,
        count(*) as co_mention_count
    FROM gold.entity_affiliations_complete e
    JOIN gold.entity_to_newsapi n ON e.entity_name = n.entity_name
    WHERE source_published_at IS NOT NULL
    AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
    AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
    GROUP BY entity_name, affiliated_entity_name, quarter
)
SELECT
    quarter,
    entity_name,
    affiliated_entity_name,
    avg_match_score,
    co_mention_count,
    rank() OVER (PARTITION BY quarter, entity_name ORDER BY avg_match_score DESC) as rank
FROM entity_mentions
WHERE entity_name = {{entity_name}}
QUALIFY rank <= 5
ORDER BY quarter, avg_match_score DESC;

-- Title: Emerging Entity Relationships
-- Description: This query identifies emerging relationships between entities based on recent news mentions.
WITH recent_mentions AS (
    SELECT
        entity_name,
        affiliated_entity_name,
        count(*) as recent_co_mentions
    FROM gold.entity_affiliations_complete e
    JOIN gold.entity_to_newsapi n ON e.entity_name = n.entity_name
    WHERE source_published_at >= current_date - interval '{{recent_period_days}} days'
    GROUP BY entity_name, affiliated_entity_name
),
historical_mentions AS (
    SELECT
        entity_name,
        affiliated_entity_name,
        count(*) as historical_co_mentions
    FROM gold.entity_affiliations_complete e
    JOIN gold.entity_to_newsapi n ON e.entity_name = n.entity_name
    WHERE source_published_at < current_date - interval '{{recent_period_days}} days'
    AND source_published_at >= current_date - interval '{{historical_period_days}} days'
    GROUP BY entity_name, affiliated_entity_name
)
SELECT
    r.entity_name,
    r.affiliated_entity_name,
    r.recent_co_mentions,
    COALESCE(h.historical_co_mentions, 0) as historical_co_mentions,
    r.recent_co_mentions - COALESCE(h.historical_co_mentions, 0) as absolute_growth,
    CASE
        WHEN COALESCE(h.historical_co_mentions, 0) = 0 THEN NULL
        ELSE round(((r.recent_co_mentions - h.historical_co_mentions) / h.historical_co_mentions::float) * 100, 2)
    END as percentage_growth
FROM recent_mentions r
LEFT JOIN historical_mentions h ON r.entity_name = h.entity_name AND r.affiliated_entity_name = h.affiliated_entity_name
WHERE ({{entity_type}} = 'All' OR r.entity_name IN (
    SELECT entity_name FROM gold.entity_affiliations_complete WHERE entity_type = {{entity_type}}
))
ORDER BY
    CASE WHEN COALESCE(h.historical_co_mentions, 0) = 0 THEN r.recent_co_mentions ELSE percentage_growth END DESC
LIMIT 20;
