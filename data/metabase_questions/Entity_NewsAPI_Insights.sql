-- Title: Entity Match Quality Distribution
-- Description: This query shows the distribution of match quality scores for entity affiliations.
SELECT CASE
         WHEN match_score >= 95 THEN 'Excellent (95-100)'
         WHEN match_score >= 90 THEN 'Very Good (90-94)'
         WHEN match_score >= 85 THEN 'Good (85-89)'
         WHEN match_score >= 80 THEN 'Acceptable (80-84)'
         ELSE 'Below Threshold (<80)'
       END as match_quality,
       count(*) as match_count
FROM gold.entity_affiliations_complete
GROUP BY match_quality
ORDER BY min(match_score) DESC;

-- Title: Entity News Source Timeline
-- Description: This query provides a timeline of news sources mentioning the specified entity.
SELECT entity_name,
       source_name,
       source_title,
       source_published_at,
       match_score
FROM gold.entity_to_newsapi
WHERE entity_name = {{entity_name}}
  AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
ORDER BY source_published_at;

-- Title: News Source Distribution by Entity Type
-- Description: This query shows how different types of entities are distributed across news sources.
SELECT entity_type,
       source_name,
       count(*) as mention_count
FROM gold.entity_to_newsapi
WHERE ({{entity_type}} = 'All' OR entity_type = {{entity_type}})
  AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY entity_type, source_name
ORDER BY entity_type, mention_count DESC;

-- Title: Entity Affiliation Network Metrics
-- Description: This query calculates network metrics for entities based on their affiliations.
SELECT entity_name,
       count(distinct affiliated_entity_name) as connection_count,
       avg(match_score) as avg_connection_strength,
       max(match_score) as strongest_connection,
       min(match_score) as weakest_connection
FROM gold.entity_affiliations_complete
WHERE ({{entity_type}} = 'All' OR entity_type = {{entity_type}})
GROUP BY entity_name
HAVING count(distinct affiliated_entity_name) > 0
ORDER BY connection_count DESC, avg_connection_strength DESC
LIMIT 20;

-- Title: Entity Mention Frequency Over Time
-- Description: This query shows how frequently entities are mentioned in news sources over time.
SELECT date_trunc('week', source_published_at) as week,
       entity_name,
       count(*) as mention_count
FROM gold.entity_to_newsapi
WHERE entity_name IN (
    SELECT entity_name
    FROM gold.entity_to_newsapi
    WHERE ({{entity_type}} = 'All' OR entity_type = {{entity_type}})
    GROUP BY entity_name
    ORDER BY count(*) DESC
    LIMIT 5
)
AND source_published_at IS NOT NULL
AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY week, entity_name
ORDER BY week, mention_count DESC;

-- Title: Entity Similarity Analysis
-- Description: This query identifies entities that are most similar based on their match scores.
SELECT e1.entity_name as entity1,
       e2.affiliated_entity_name as entity2,
       avg(e1.match_score) as similarity_score,
       count(*) as common_affiliations
FROM gold.entity_affiliations_complete e1
JOIN gold.entity_affiliations_complete e2
  ON e1.affiliated_entity_name = e2.affiliated_entity_name
  AND e1.entity_name != e2.entity_name
  AND e1.entity_name != e2.affiliated_entity_name
  AND e2.entity_name != e1.affiliated_entity_name
WHERE ({{entity_type}} = 'All' OR e1.entity_type = {{entity_type}})
GROUP BY entity1, entity2
HAVING count(*) > 1
ORDER BY similarity_score DESC, common_affiliations DESC
LIMIT 20;

-- Title: News Source Influence by Entity Coverage
-- Description: This query measures the influence of news sources based on the number of entities they cover.
SELECT source_name,
       count(distinct entity_name) as entities_covered,
       count(*) as total_mentions,
       avg(match_score) as avg_match_quality
FROM gold.entity_to_newsapi
WHERE ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY source_name
ORDER BY entities_covered DESC, total_mentions DESC
LIMIT 20;

-- Title: Entity Affiliation Comparison
-- Description: This query compares the affiliations of two specified entities.
WITH entity1_affiliations AS (
    SELECT affiliated_entity_name, affiliated_entity_type, match_score
    FROM gold.entity_affiliations_complete
    WHERE entity_name = {{entity_name_1}}
),
entity2_affiliations AS (
    SELECT affiliated_entity_name, affiliated_entity_type, match_score
    FROM gold.entity_affiliations_complete
    WHERE entity_name = {{entity_name_2}}
)
SELECT
    COALESCE(e1.affiliated_entity_name, e2.affiliated_entity_name) as affiliated_entity,
    COALESCE(e1.affiliated_entity_type, e2.affiliated_entity_type) as affiliated_type,
    e1.match_score as entity1_match_score,
    e2.match_score as entity2_match_score,
    CASE
        WHEN e1.match_score IS NULL THEN 'Only with ' || {{entity_name_2}}
        WHEN e2.match_score IS NULL THEN 'Only with ' || {{entity_name_1}}
        ELSE 'With both entities'
    END as affiliation_type
FROM entity1_affiliations e1
FULL OUTER JOIN entity2_affiliations e2
    ON e1.affiliated_entity_name = e2.affiliated_entity_name
ORDER BY
    CASE WHEN affiliation_type = 'With both entities' THEN 1
         WHEN affiliation_type = 'Only with ' || {{entity_name_1}} THEN 2
         ELSE 3
    END,
    COALESCE(e1.match_score, 0) + COALESCE(e2.match_score, 0) DESC;

-- Title: Entity Mention Context Analysis
-- Description: This query analyzes the context in which entities are mentioned in news sources.
SELECT entity_name,
       source_title,
       source_description,
       match_details,
       match_score
FROM gold.entity_to_newsapi
WHERE entity_name = {{entity_name}}
  AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
ORDER BY match_score DESC
LIMIT 50;

-- Title: Entity Type Correlation
-- Description: This query shows how different entity types are correlated based on their co-occurrence in affiliations.
SELECT e1.entity_type as type1,
       e2.affiliated_entity_type as type2,
       count(*) as correlation_strength
FROM gold.entity_affiliations_complete e1
JOIN gold.entity_affiliations_complete e2
  ON e1.entity_name = e2.entity_name
  AND e1.entity_type != e2.affiliated_entity_type
GROUP BY type1, type2
ORDER BY correlation_strength DESC;
