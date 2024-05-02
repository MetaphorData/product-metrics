-- Athena (Presto SQL)
WITH FollowersExpanded AS (
    SELECT
        d.id AS d_id,
        json_element AS user_id
    FROM
        metaphor_metaphor.users d
        CROSS JOIN UNNEST(cast(json_extract(d.asset_followers, '$.followedBy') as ARRAY<VARCHAR>)) AS t(json_element)
),
FollowersWithEmails AS (
    SELECT
        fe.d_id,
        array_agg(json_extract_scalar(users.logical_id, '$.email')) AS follower_emails
    FROM
        FollowersExpanded fe
        JOIN metaphor_metaphor.users users ON fe.user_id = users.id
    GROUP BY
        fe.d_id
),
RecentSearches as (
	SELECT 
		d.id as d_id,
		json_extract(activity, '$.recentSearches') AS searches
  	FROM metaphor_metaphor.users d
),
SearchKeywords AS (
    SELECT
        re.d_id as d_id,
        json_extract_scalar(search_item, '$.keyword') AS keyword
    FROM
        RecentSearches re
        CROSS JOIN UNNEST(cast(searches as ARRAY<JSON>)) as t(search_item)
),
AggregatedKeywords AS (
    SELECT
        d_id,
        array_agg(keyword) AS recent_searches_keywords 
    FROM SearchKeywords
    GROUP BY d_id
)
select 
	id,
	display_name,
	created_at,
	last_modified_at,
	json_extract_scalar(logical_id, '$.email') as email,
	json_extract_scalar(properties, '$.role') as role,
	properties,
	asset_followers,
	fe.follower_emails as asset_followers_emails,
	ak.recent_searches_keywords,
	activity 
from metaphor_metaphor.users d
	left join AggregatedKeywords ak on d.id = ak.d_id
	left join FollowersWithEmails fe on d.id = fe.d_id; 
