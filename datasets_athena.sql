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
