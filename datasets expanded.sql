-- Snowflake
WITH ExpandedTags AS (
    SELECT
        d.id AS d_id,
        t.value::STRING AS tag_id
    FROM
        datasets d,
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
        datasets d,
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
AssetContactsExpanded as (
	SELECT
        d.id AS d_id,
        PARSE_JSON(contact.value):designation::STRING AS designation_id,
        PARSE_JSON(contact.value):value::STRING AS value_id
    FROM
        datasets d,
        LATERAL FLATTEN(input => PARSE_JSON(d.asset_contacts):contacts) contact
),
AssetContactInfo as (
	SELECT
        d.id AS d_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'email', PARSE_JSON(users.logical_id):email::STRING,
                'designation', PARSE_JSON(u.user_defined_resource_info):name::STRING
            )
        ) AS contacts_info
    FROM
        datasets d,
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
	PARSE_JSON(d.logical_id):name::STRING as logical_id_name,
	PARSE_JSON(d.logical_id):platform::STRING as logical_id_platform,
	PARSE_JSON(d.schema):description::STRING as schema_description,
	PARSE_JSON(d.schema):entityId::STRING as schema_entityId,
	PARSE_JSON(d.schema):fields::STRING as schema_fields,
	PARSE_JSON(d.schema):sqlSchema.materialization::STRING as schema_sqlSchema_materialization,
	PARSE_JSON(d.structure):database::STRING as structure_database,
	PARSE_JSON(d.structure):schema::STRING as structure_schema,
	PARSE_JSON(d.structure):table::STRING as structure_table,
	PARSE_JSON(d.statistics):dataSizeBytes::STRING as statistics_dataSizeBytes,
	PARSE_JSON(d.statistics):lastUpdated::STRING as statistics_lastUpdated,
	PARSE_JSON(d.statistics):recordCount::STRING as statistics_recordCount,
	PARSE_JSON(d.source_info):mainUrl::STRING as source_info_mainUrl,
	PARSE_JSON(d.entity_upstream):sourceDatasets::STRING as upstream_sourceDatasets,
	PARSE_JSON(d.asset_followers):followedBy::STRING as asset_followers_followedBy,
	fe.follower_emails as asset_followers_emails,
	PARSE_JSON(d.asset_contacts):contacts::STRING as asset_contacts_contacts,
	aci.contacts_info as asset_contacts_info,
	PARSE_JSON(d.asset_governed_tags):governedTagIds::STRING as asset_governed_tags_governedTagIds,
	ARRAY_AGG(tn.tag_name) OVER (PARTITION BY tn.d_id) AS governedTagIds_tag_names
FROM
	datasets d
	LEFT JOIN TagsWithNames tn ON d.id = tn.d_id
	LEFT JOIN FollowersWithEmails fe ON d.id = fe.d_id
    	LEFT JOIN AssetContactInfo aci ON d.id = aci.d_id;



-- Athena (Presto SQL)
WITH ExpandedTags AS (
    SELECT
        d.id AS d_id,
        element AS tag_id
    FROM
        metaphor_metaphor.datasets d
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
        metaphor_metaphor.datasets d
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
        metaphor_metaphor.datasets d
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
        metaphor_metaphor.datasets d
        CROSS JOIN UNNEST(cast(json_extract(d.asset_contacts, '$.contacts') AS ARRAY<JSON>)) AS contact(value)
        JOIN metaphor_metaphor.user_defined_resource u ON json_extract_scalar(contact.value, '$.designation') = u.id
        JOIN metaphor_metaphor.users users ON json_extract_scalar(contact.value, '$.value') = users.id
    GROUP BY
        d.id
)
SELECT
	d.id,
	d.entity_type,
	d.created_at,
	d.last_modified_at,
	d.last_ingested_at,
	json_extract_scalar(d.logical_id,
	'$.name') as logical_id_name,
	json_extract_scalar(d.logical_id,
	'$.platform') as logical_id_platform,
	json_extract_scalar(d.schema,
	'$.description') as schema_description,
	json_extract_scalar(d.schema,
	'$.entityId') as schema_entityId,
	json_extract(d.schema,
	'$.fields') as schema_fields,
	json_extract_scalar(d.schema,
	'$.sqlSchema.materialization') as schema_sqlSchema_materialization,
	json_extract_scalar(d.structure,
	'$.database') as structure_database,
	json_extract_scalar(d.structure,
	'$.schema') as structure_schema,
	json_extract_scalar(d.structure,
	'$.table') as structure_table,
	json_extract_scalar(d.statistics,
	'$.dataSizeBytes') as statistics_dataSizeBytes,
	json_extract_scalar(d.statistics,
	'$.lastUpdated') as statistics_lastUpdated,
	json_extract_scalar(d.statistics,
	'$.recordCount') as statistics_recordCount,
	json_extract_scalar(d.source_info,
	'$.mainUrl') as source_info_mainUrl,
	json_extract(d.upstream,
	'$.sourceDatasets') as upstream_sourceDatasets,
	json_extract(d.asset_followers ,
	'$.followedBy') as asset_followers_followedBy,
	fe.follower_emails as asset_followers_emails,
	json_extract(d.asset_contacts ,
	'$.contacts') as asset_contacts_contacts,
	aci.contacts_info as asset_contacts_info,
	json_extract(d.asset_governed_tags,
	'$.governedTagIds') as asset_governed_tags_governedTagIds,
	array_agg(tag_name) OVER (PARTITION BY tn.d_id) AS governedTagIds_tag_names,
	json_extract(d.custom_metadata,
	'$.metadata') as custom_metadata_metadata
from
	metaphor_metaphor.datasets d
	left join TagsWithNames tn on d.id = tn.d_id
	left join FollowersWithEmails fe on d.id = fe.d_id
   	left join AssetContactInfo aci on d.id = aci.d_id
;
