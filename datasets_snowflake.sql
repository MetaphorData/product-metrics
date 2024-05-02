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



