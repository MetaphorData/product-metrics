-- Snowflake
WITH ExpandedTags AS (
    SELECT
        d.id AS d_id,
        t.value::STRING AS tag_id
    FROM
        dashboards d,
        LATERAL FLATTEN(input => PARSE_JSON(d.asset_governed_tags):governedTagIds) t
),
TagsWithNames AS (
    SELECT
        et.d_id,
        PARSE_JSON(u.user_defined_resource_info):name::STRING AS tag_name
    FROM
        ExpandedTags et
        JOIN user_defined_resource u ON et.tag_id = u.id::STRING
),
FollowersExpanded AS (
    SELECT
        d.id AS d_id,
        t.value::STRING AS user_id
    FROM
        dashboards d,
        LATERAL FLATTEN(input => PARSE_JSON(d.asset_followers):followedBy) t
),
FollowersWithEmails AS (
    SELECT
        fe.d_id,
        ARRAY_AGG(PARSE_JSON(users.logical_id):email::STRING) AS follower_emails
    FROM
        FollowersExpanded fe
        JOIN users ON fe.user_id = users.id
    GROUP BY
        fe.d_id
),
AssetContactsExpanded AS (
    SELECT
        d.id AS d_id,
        PARSE_JSON(contact.value):designation::STRING AS designation_id,
        PARSE_JSON(contact.value):value::STRING AS value_id
    FROM
        dashboards d,
        LATERAL FLATTEN(input => PARSE_JSON(d.asset_contacts):contacts) contact
),
AssetContactInfo AS (
    SELECT
        d.id AS d_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'email', PARSE_JSON(users.logical_id):email::STRING,
                'designation', PARSE_JSON(u.user_defined_resource_info):name::STRING
            )
        ) AS contacts_info
    FROM
        dashboards d,
        LATERAL FLATTEN(input => PARSE_JSON(d.asset_contacts):contacts) contact
        JOIN user_defined_resource u ON PARSE_JSON(contact.value):designation::STRING = u.id
        JOIN users ON PARSE_JSON(contact.value):value::STRING = users.id
    GROUP BY
        d.id
)
SELECT
    d.id, 
    d.entity_type, 
    d._created_at, 
    d.last_modified_at, 
    d.last_ingested_at, 
    d.source_info, 
    d.logical_id, 
    d.entity_upstream, 
    d.dashboard_info, 
    PARSE_JSON(d.dashboard_info):description::STRING as dashboard_info_description,
    PARSE_JSON(d.dashboard_info):powerBi.powerBiDashboardType::STRING as dashboard_powerBI,
    d.asset_followers, 
    PARSE_JSON(d.asset_followers):followedBy::STRING as asset_followers_followedBy,
    fe.follower_emails as asset_followers_emails,
    d.asset_contacts, 
    PARSE_JSON(d.asset_contacts):contacts::STRING as asset_contacts_contacts,
    aci.contacts_info as asset_contacts_info,
    d.asset_governed_tags, 
    PARSE_JSON(d.asset_governed_tags):governedTagIds::STRING as asset_governed_tags_governedTagIds,
    ARRAY_AGG(tn.tag_name) OVER (PARTITION BY tn.d_id) AS governedTagIds_tag_names,
    PARSE_JSON(d.structure):name::STRING as structure_name,
    PARSE_JSON(d.structure):directories::STRING as structure_directories
FROM
    dashboards d
    LEFT JOIN TagsWithNames tn ON d.id = tn.d_id
    LEFT JOIN FollowersWithEmails fe ON d.id = fe.d_id
    LEFT JOIN AssetContactInfo aci ON d.id = aci.d_id;



-- Athena (Presto SQL)
WITH ExpandedTags AS (
    SELECT
        d.id AS d_id,
        element AS tag_id
    FROM
        metaphor_metaphor.dashboards d
        CROSS JOIN UNNEST(cast(json_extract(d.asset_governed_tags, '$.governedTagIds') as ARRAY<VARCHAR>)) AS t(element)
),
TagsWithNames AS (
    SELECT
        et.d_id,
        json_extract_scalar(u.user_defined_resource_info, '$.name') AS tag_name
    FROM
        ExpandedTags et
        JOIN metaphor_metaphor.user_defined_resource u ON et.tag_id = CAST(u.id AS VARCHAR)
),
FollowersExpanded AS (
    SELECT
        d.id AS d_id,
        json_element AS user_id
    FROM
        metaphor_metaphor.dashboards d
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
AssetContactsExpanded as (
	SELECT
        d.id AS d_id,
        json_extract_scalar(contact.value, '$.designation') AS designation_id,
        json_extract_scalar(contact.value, '$.value') AS value_id
    FROM
        metaphor_metaphor.dashboards d
        CROSS JOIN UNNEST(cast(json_extract(json_parse(d.asset_contacts), '$.contacts') AS ARRAY<JSON>)) AS contact(value)
),
AssetContactInfo as (
	SELECT
        d.id AS d_id,
        array_agg(
            JSON_OBJECT(
                'email' VALUE json_extract_scalar(users.logical_id, '$.email'),
                'designation' VALUE json_extract_scalar(u.user_defined_resource_info, '$.name')
            )
        ) AS contacts_info
    FROM
        metaphor_metaphor.dashboards d
        CROSS JOIN UNNEST(cast(json_extract(d.asset_contacts, '$.contacts') AS ARRAY<JSON>)) AS contact(value)
        JOIN metaphor_metaphor.user_defined_resource u ON json_extract_scalar(contact.value, '$.designation') = u.id
        JOIN metaphor_metaphor.users users ON json_extract_scalar(contact.value, '$.value') = users.id
    GROUP BY
        d.id
)
select 
	id, 
	entity_type, 
	created_at, 
	last_modified_at, 
	last_ingested_at, 
	source_info, 
	logical_id, 
	upstream, 
	dashboard_info, 
	json_extract(d.dashboard_info ,
		'$.description') as dashboard_info_description,
	json_extract(d.dashboard_info ,
		'$.powerBi.powerBiDashboardType') as dashboard_powerBI,
	asset_followers, 
	json_extract(d.asset_followers ,
		'$.followedBy') as asset_followers_followedBy,
	fe.follower_emails as asset_followers_emails,
	asset_contacts, 
	json_extract(d.asset_contacts ,
		'$.contacts') as asset_contacts_contacts,
	aci.contacts_info as asset_contacts_info,
	asset_governed_tags, 
	json_extract(d.asset_governed_tags,
		'$.governedTagIds') as asset_governed_tags_governedTagIds,
	array_agg(tag_name) OVER (PARTITION BY tn.d_id) AS governedTagIds_tag_names,
	"structure",
	json_extract(d."structure",
		'$.name') as structure_name,
	json_extract(d."structure",
		'$.directories') as structure_directories
from metaphor_metaphor.dashboards d
	left join TagsWithNames tn on d.id = tn.d_id
	left join FollowersWithEmails fe on d.id = fe.d_id
  	left join AssetContactInfo aci on d.id = aci.d_id;

   
