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


   
