-- Title: Entity Name
-- Description: This query retrieves information about entities and their affiliations based on the provided entity name and type.
SELECT distinct entity_name
FROM gold.entity_affiliations_complete
WHERE entity_name LIKE {{entity_name}};

-- Title: Entity Description
-- Description: This query retrieves the description of entities based on the provided entity name.
SELECT distinct entity_description
FROM gold.entity_affiliations_complete
WHERE entity_name LIKE {{entity_name}};

-- Entity Affiliations
-- Description: This query retrieves affiliated entities and their descriptions based on the provided entity name and type.
SELECT distinct affiliated_entity_name,
                affiliated_entity_description
FROM gold.entity_affiliations_complete
WHERE entity_name = {{entity_name}}
  AND affiliated_entity_type = {{entity_type}};

-- Title: Total Affiliations
-- Description: This query counts the total number of affiliations for a given entity name grouped by the type of affiliated entity.
SELECT distinct affiliated_entity_type,
                count(affiliated_entity_name) as total_affiliations
FROM gold.entity_affiliations_complete
WHERE entity_name = {{entity_name}}
GROUP BY affiliated_entity_type
ORDER BY total_affiliations DESC;

-- Title: Entity Affiliations by Type
-- Description: This query retrieves affiliated entities and their descriptions based on the provided entity name and type, grouped by the type of affiliated entity.
SELECT distinct affiliated_entity_type,
                affiliated_entity_name,
                affiliated_entity_description
FROM gold.entity_affiliations_complete
WHERE entity_name = {{entity_name}}
  AND affiliated_entity_type = {{entity_type}}
ORDER BY affiliated_entity_name;
