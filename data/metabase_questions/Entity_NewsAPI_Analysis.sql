-- Title: Entity Information
-- Description: This query retrieves basic information about entities based on the provided entity name.
SELECT distinct entity_name, entity_type, entity_description
FROM gold.entity_affiliations_complete
WHERE entity_name LIKE {{entity_name}};

-- Title: Entity Affiliations
-- Description: This query retrieves affiliated entities based on the provided entity name and optional entity type filter.
SELECT distinct affiliated_entity_name,
                affiliated_entity_type,
                affiliated_entity_description,
                match_score
FROM gold.entity_affiliations_complete
WHERE entity_name = {{entity_name}}
  AND ({{entity_type}} = 'All' OR affiliated_entity_type = {{entity_type}})
ORDER BY match_score DESC;

-- Title: Entity Affiliations by Type
-- Description: This query counts the number of affiliated entities by type for a given entity.
SELECT distinct affiliated_entity_type,
                count(affiliated_entity_name) as total_affiliations
FROM gold.entity_affiliations_complete
WHERE entity_name = {{entity_name}}
GROUP BY affiliated_entity_type
ORDER BY total_affiliations DESC;

-- Title: Entity Affiliations Over Time
-- Description: This query shows the distribution of entity affiliations over time based on match score.
SELECT date_trunc('month', source_published_at) as month,
       count(*) as total_mentions,
       avg(match_score) as avg_match_score
FROM gold.entity_to_newsapi
WHERE entity_name = {{entity_name}}
  AND source_published_at IS NOT NULL
GROUP BY month
ORDER BY month;

-- Title: Entity News Sources
-- Description: This query retrieves news sources that mention the specified entity.
SELECT distinct source_name,
                source_title,
                source_description,
                source_url,
                source_published_at,
                match_score
FROM gold.entity_to_newsapi
WHERE entity_name = {{entity_name}}
  AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
ORDER BY source_published_at DESC;

-- Title: Top Entities by News Mentions
-- Description: This query shows the entities with the most news mentions.
SELECT entity_name,
       entity_type,
       count(distinct source_uri) as mention_count
FROM gold.entity_to_newsapi
WHERE ({{entity_type}} = 'All' OR entity_type = {{entity_type}})
  AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY entity_name, entity_type
ORDER BY mention_count DESC
LIMIT 20;

-- Title: Entity Relationship Network
-- Description: This query provides data for visualizing the network of entity relationships.
SELECT entity_name as source_entity,
       entity_type as source_type,
       affiliated_entity_name as target_entity,
       affiliated_entity_type as target_type,
       match_score as relationship_strength
FROM gold.entity_affiliations_complete
WHERE entity_name = {{entity_name}}
   OR affiliated_entity_name = {{entity_name}}
ORDER BY match_score DESC;

-- Title: Entity News Sentiment Over Time
-- Description: This query shows the sentiment of news mentions for an entity over time (assuming a sentiment field exists).
SELECT date_trunc('week', source_published_at) as week,
       count(*) as mention_count,
       avg(match_score) as avg_relevance
FROM gold.entity_to_newsapi
WHERE entity_name = {{entity_name}}
  AND ({{date_from}} IS NULL OR source_published_at >= {{date_from}})
  AND ({{date_to}} IS NULL OR source_published_at <= {{date_to}})
GROUP BY week
ORDER BY week;

-- Title: Related Entities Co-occurrence
-- Description: This query shows which entities frequently appear together in news sources.
WITH entity_sources AS (
    SELECT entity_name, source_uri
    FROM gold.entity_to_newsapi
    WHERE ({{entity_type}} = 'All' OR entity_type = {{entity_type}})
)
SELECT e1.entity_name as entity1,
       e2.entity_name as entity2,
       count(distinct e1.source_uri) as co_occurrence_count
FROM entity_sources e1
JOIN entity_sources e2 ON e1.source_uri = e2.source_uri AND e1.entity_name < e2.entity_name
GROUP BY entity1, entity2
HAVING count(distinct e1.source_uri) > 1
ORDER BY co_occurrence_count DESC
LIMIT 20;

-- Title: Entity Type Distribution
-- Description: This query shows the distribution of entity types in the database.
SELECT entity_type,
       count(distinct entity_name) as entity_count
FROM gold.entity_affiliations_complete
GROUP BY entity_type
ORDER BY entity_count DESC;
