WITH
-- Step 1: Precompute parsed JSON fields to avoid repeated parsing
    DatasetsParsed AS (
        SELECT
            d.id,
            d.entity_type,
            d._created_at,
            d.last_modified_at,
            d.last_ingested_at,
            d.is_non_prod,
            PARSE_JSON(d.logical_id) AS logical_id_json,
            PARSE_JSON(d.schema) AS schema_json,
            PARSE_JSON(d.structure) AS structure_json,
            PARSE_JSON(d.statistics) AS statistics_json,
            PARSE_JSON(d.source_info) AS source_info_json,
            PARSE_JSON(d.entity_upstream) AS entity_upstream_json,
            PARSE_JSON(d.asset_followers) AS asset_followers_json,
            PARSE_JSON(d.asset_contacts) AS asset_contacts_json,
            PARSE_JSON(d.asset_governed_tags) AS asset_governed_tags_json
        FROM
            datasets d
    ),
    -- Step 2: Expand governed tags and join with user defined resource
    ExpandedTags AS (
        SELECT
            dp.id AS d_id,
            tag.value::STRING AS tag_id
        FROM
            DatasetsParsed dp,
            LATERAL FLATTEN(input => dp.asset_governed_tags_json:governedTagIds) tag
    ),
    TagsWithNames AS (
        SELECT
            et.d_id,
            u.user_defined_resource_info:name::STRING AS tag_name
        FROM
            ExpandedTags et
            JOIN user_defined_resource u ON et.tag_id = u.id::STRING
    ),
    -- Step 3: Expand followers and join with users
    FollowersExpanded AS (
        SELECT
            dp.id AS d_id,
            follower.value::STRING AS user_id
        FROM
            DatasetsParsed dp,
            LATERAL FLATTEN(input => dp.asset_followers_json:followedBy) follower
    ),
    FollowersWithEmails AS (
        SELECT
            fe.d_id,
            ARRAY_AGG(users.logical_id:email::STRING) AS follower_emails
        FROM
            FollowersExpanded fe
            JOIN users ON fe.user_id = users.id
        GROUP BY
            fe.d_id
    ),
    -- Step 4: Expand asset contacts and join with users and user defined resource
    AssetContactsExpanded AS (
        SELECT
            dp.id AS d_id,
            contact.value AS contact_value
        FROM
            DatasetsParsed dp,
            LATERAL FLATTEN(input => dp.asset_contacts_json:contacts) contact
    ),
    AssetContactInfo AS (
        SELECT
            ace.d_id,
            ARRAY_AGG(
                OBJECT_CONSTRUCT(
                    'email', users.logical_id:email::STRING,
                    'designation', u.user_defined_resource_info:name::STRING
                )
            ) AS contacts_info
        FROM
            AssetContactsExpanded ace
            JOIN user_defined_resource u ON PARSE_JSON(ace.contact_value):designation::STRING = u.id
            JOIN users ON PARSE_JSON(ace.contact_value):value::STRING = users.id
        GROUP BY
            ace.d_id
    ),
    SchemaDescription AS(
        SELECT dp.id AS d_id
            ,IFF(dp.schema_json:description IS NOT NULL AND dp.schema_json:description != '',
                ARRAY_CONSTRUCT(OBJECT_CONSTRUCT(
                    'description', dp.schema_json:description::STRING,
                    'source', dp.logical_id_json:platform::STRING
            )), NULL) AS schema_desc
        FROM DatasetsParsed dp
    ),
    UserDescription AS(
        SELECT dp.id AS d_id
            ,IFF(kc.knowledge_card_info:detail:assetDescription:description IS NOT NULL,
                ARRAY_CONSTRUCT(OBJECT_CONSTRUCT(
                    'description', kc.knowledge_card_info:detail:assetDescription:description::STRING,
                    'source', u.logical_id:email::STRING
                )), NULL) AS user_desc
        FROM DatasetsParsed dp
            LEFT JOIN KNOWLEDGE_CARDS kc ON dp.id = kc.knowledge_card_info:anchorEntityId
                AND kc.knowledge_card_info:detail:type = 'ASSET_DESCRIPTION'
            LEFT JOIN USERS u ON kc.knowledge_card_info:created:actor = u.id
    ),
    AllDescriptions AS(
        SELECT sd.d_id
            ,IFF(sd.schema_desc IS NOT NULL OR ud.user_desc IS NOT NULL,
            ARRAY_CAT(
                COALESCE(sd.schema_desc, []), COALESCE(ud.user_desc, [])
            ), NULL) AS descriptions
        FROM SchemaDescription sd
            LEFT JOIN UserDescription ud ON sd.d_id = ud.d_id
    )
-- Final Select
SELECT
    dp.id,
    dp.entity_type,
    dp._created_at,
    dp.last_modified_at,
    dp.last_ingested_at,
    dp.is_non_prod,
    dp.logical_id_json:name::STRING AS logical_id_name,
    dp.logical_id_json:platform::STRING AS logical_id_platform,
    -- dp.schema_json:description::STRING AS schema_description,
    ad.descriptions::STRING AS schema_description,
    dp.schema_json:entityId::STRING AS schema_entityId,
    dp.schema_json:fields::STRING AS schema_fields,
    dp.schema_json:sqlSchema.materialization::STRING AS schema_sqlSchema_materialization,
    dp.structure_json:database::STRING AS structure_database,
    dp.structure_json:schema::STRING AS structure_schema,
    dp.structure_json:table::STRING AS structure_table,
    dp.statistics_json:dataSizeBytes::STRING AS statistics_dataSizeBytes,
    dp.statistics_json:lastUpdated::STRING AS statistics_lastUpdated,
    dp.statistics_json:recordCount::STRING AS statistics_recordCount,
    dp.source_info_json:mainUrl::STRING AS source_info_mainUrl,
    dp.entity_upstream_json:sourceDatasets::STRING AS upstream_sourceDatasets,
    dp.asset_followers_json:followedBy::STRING AS asset_followers_followedBy,
    fe.follower_emails AS asset_followers_emails,
    dp.asset_contacts_json:contacts::STRING AS asset_contacts_contacts,
    aci.contacts_info AS asset_contacts_info,
    dp.asset_governed_tags_json:governedTagIds::STRING AS asset_governed_tags_governedTagIds,
    ARRAY_AGG(tn.tag_name) OVER (PARTITION BY tn.d_id) AS governedTagIds_tag_names
FROM
    DatasetsParsed dp
    LEFT JOIN TagsWithNames tn ON dp.id = tn.d_id
    LEFT JOIN FollowersWithEmails fe ON dp.id = fe.d_id
    LEFT JOIN AssetContactInfo aci ON dp.id = aci.d_id
    LEFT JOIN AllDescriptions ad ON dp.id = ad.d_id
WHERE
    SCHEMA_FIELDS is not null AND
    IS_NON_PROD = FALSE
ORDER BY schema_description