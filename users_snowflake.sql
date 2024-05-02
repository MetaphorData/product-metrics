-- Snowflake
WITH AggregatedKeywords AS (
    SELECT
        d.id as d_id,
        ARRAY_AGG(keyword.value:keyword::STRING) AS recent_searches_keywords 
    FROM 
        users d,
        LATERAL FLATTEN(input => PARSE_JSON(activity):recentSearches) AS keyword
    GROUP BY d.id
)
SELECT 
    d.id,
    d.display_name,
    d._created_at,
    d.last_modified_at,
    PARSE_JSON(d.logical_id):email::STRING as email,
    PARSE_JSON(d.properties):role::STRING as role,
    d.properties,
    ak.recent_searches_keywords,
    d.activity
FROM users d
    LEFT JOIN AggregatedKeywords ak ON d.id = ak.d_id;



