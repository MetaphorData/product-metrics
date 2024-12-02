WITH
    -- Step 1: Precompute parsed JSON fields to avoid repeated parsing
    NamespacesParsed AS (
        SELECT
            id, 
            _created_at,
            last_modified_at,
            deleted_at,
            PARSE_JSON(n.asset_followers) AS asset_followers_json,
            PARSE_JSON(n.asset_contacts) AS asset_contacts_json,
            PARSE_JSON(n.asset_governed_tags) AS asset_governed_tags_json,
            PARSE_JSON(NAMESPACE_INFO) AS AssetNamespace_info_json,
            PARSE_JSON(NAMESPACE_ASSETS) AS AssetNamespace_assets_json
        FROM 
            namespaces n
    ),
    
    -- Step 2: Expand governed tags and join with user-defined resource
    ExpandedTags AS (
        SELECT
            dp.id AS d_id,
            tag.value::STRING AS tag_id
        FROM
            NamespacesParsed dp,
            LATERAL FLATTEN(input => dp.asset_governed_tags_json:governedTagIds) tag
    ),
    TagsWithNames AS (
        SELECT
            et.d_id,
            et.tag_id,
            u.user_defined_resource_info:name::STRING AS tag_name,
            u.user_defined_resource_info:description:text::STRING AS tag_description
        FROM
            ExpandedTags et
            JOIN user_defined_resource u ON et.tag_id = u.id::STRING
    ),
    TagsAggregated AS (
        SELECT
            d_id,
            ARRAY_AGG(
                OBJECT_CONSTRUCT(
                    'tag_id', tag_id,
                    'tag_name', tag_name,
                    'tag_description', tag_description
                )
            ) AS tags
        FROM TagsWithNames
        GROUP BY d_id
    ),
    -- Step 3: Expand followers and join with users
    FollowersExpanded AS (
        SELECT
            dp.id AS d_id,
            follower.value::STRING AS user_id
        FROM
            NamespacesParsed dp,
            LATERAL FLATTEN(input => dp.asset_followers_json:followedBy) follower
    ),
    FollowersWithEmails AS (
        SELECT
            fe.d_id,
            ARRAY_AGG(OBJECT_CONSTRUCT(
                'name', users.display_name,
                'email', users.logical_id:email::STRING
            )) AS follower_emails
        FROM
            FollowersExpanded fe
            JOIN users ON fe.user_id = users.id
        GROUP BY
            fe.d_id
    ),
    -- Step 4: Expand asset contacts and join with users and user-defined resource
    AssetContactsExpanded AS (
        SELECT
            dp.id AS d_id,
            contact.value AS contact_value
        FROM
            NamespacesParsed dp,
            LATERAL FLATTEN(input => dp.asset_contacts_json:contacts) contact
    ),
    AssetContactInfo AS (
        SELECT
            ace.d_id,
            ARRAY_AGG(
                OBJECT_CONSTRUCT(
                    'name', users.display_name,
                    'email', users.logical_id:email::STRING
                )
            ) AS contacts_info
        FROM
            AssetContactsExpanded ace
            JOIN user_defined_resource u ON PARSE_JSON(ace.contact_value):designation::STRING = u.id
            JOIN users ON PARSE_JSON(ace.contact_value):value::STRING = users.id
        GROUP BY
            ace.d_id
    ),
    -- Step 5: Get nested asset IDs from namespace_assets:namedAssetCollections
    AssetHierarchy AS (
        SELECT 
            da._id AS id,
            ns.AssetNamespace_info_json:name::STRING AS name,
            da.parent,
            ARRAY_CAT(da.live_query_assets, da.selected_assets) AS assets
        FROM domain_assets da
        LEFT JOIN namespacesparsed ns ON da._id = ns.id
        UNION ALL
        SELECT 
            da._id,
            ns.AssetNamespace_info_json:name::STRING,
            da.parent,
            ARRAY_CAT(da.live_query_assets, da.selected_assets)
        FROM domain_assets da
        JOIN AssetHierarchy ah ON ah.id = da.parent
        LEFT JOIN namespacesparsed ns ON da._id = ns.id
    ),
    AssetDetails AS (
        SELECT DISTINCT 
            id,
            name,
            parent,
            asset.value::STRING AS asset
        FROM AssetHierarchy,
        LATERAL FLATTEN(input => assets) asset
    ),
    AssetNames AS (
        SELECT
            ad.id,
            ARRAY_AGG(OBJECT_CONSTRUCT(
                'asset_id', ad.asset,
                'name', COALESCE(
                    dash.dashboard_info:title::STRING,
                    data.logical_id:name::STRING,
                    vv.structure:name::STRING
                ),
                'workspace', 
                NULLIF(dash.logical_id:platform::STRING, '')
            )) AS names
        FROM AssetDetails ad
            LEFT JOIN dashboards dash ON ad.asset = dash.id
            LEFT JOIN datasets data ON ad.asset = data.id
            LEFT JOIN knowledge_cards kc ON ad.asset = kc.id
            LEFT JOIN virtual_views vv ON ad.asset = vv.id
        GROUP BY ad.id
    ),
    -- Step 7: Get information about the user who created the namespace
    CreatedByUser AS (
        SELECT 
            np.id,
            ARRAY_AGG(OBJECT_CONSTRUCT(
                'name', u.display_name,
                'email', u.logical_id:email::STRING
            )) AS user_info
        FROM 
            NamespacesParsed np
            LEFT JOIN users u ON np.AssetNamespace_info_json:created:actor::STRING = u.id
        GROUP BY 
            np.id
    ),
    AssetDescription AS(
        SELECT np.id,
            kc.knowledge_card_info:detail:assetDescription:description::STRING description
        FROM NAMESPACESPARSED np
            LEFT JOIN KNOWLEDGE_CARDS kc ON np.id = kc.knowledge_card_info:anchorEntityId
                AND kc.knowledge_card_info:detail:assetDescription:description IS NOT NULL
    ),
    TopQueries AS(
        SELECT ad.id,
        NULLIF(ARRAY_AGG(NULLIF(OBJECT_CONSTRUCT(
            'title', kc.knowledge_card_info:detail:query:title,
            'explanation', kc.knowledge_card_info:detail:query:explanations[0]:explanation,
            'query', kc.knowledge_card_info:detail:query:query
        ), {})), []) AS queries
        FROM AssetDetails ad
            LEFT JOIN KNOWLEDGE_CARDS kc
                ON ad.asset = kc.knowledge_card_info:anchorEntityId
                AND knowledge_card_info:detail:type = 'QUERY_DESCRIPTION'
                AND deleted_at IS NULL
                AND knowledge_card_info:detail:query:isMarkedAsCurated = TRUE
        GROUP BY ad.id
    )
SELECT
    -- Domain Name (top level)
    IFF(np.AssetNamespace_info_json:detail:type::STRING = 'DATA_GROUP', 
        IFF(np.AssetNamespace_info_json:parentId IS NULL, 
            np.AssetNamespace_info_json:name::STRING, 
            ''
        ), 
        ''
    ) AS domain_name,
    
    -- SubDomain Name (child)
    IFF(np.AssetNamespace_info_json:detail:type::STRING = 'DATA_GROUP',
        IFF(np.AssetNamespace_info_json:parentId IS NOT NULL, 
            np.AssetNamespace_info_json:name::STRING, 
            ''
        ), 
        ''
    ) AS subdomain_name,
    
    -- Data Product Name (child)
    IFF(np.AssetNamespace_info_json:detail:type::STRING = 'DATA_UNIT',
        np.AssetNamespace_info_json:name::STRING, 
        ''
    ) AS data_product_name,
    
    -- Domain ID
    np.id AS domain_id,
    
    -- Last modified 
    np.last_modified_at AS last_modified,
    
    -- Created by
    cbu.user_info AS created_by,
    
    -- Created at
    np.AssetNamespace_info_json:created:time::STRING AS created_at,
    
    -- Contacts
    aci.contacts_info AS contacts,
    
    -- Tags
    ta.tags AS asset_tags,
    
    -- Description
    COALESCE(np.AssetNamespace_info_json:description:text::STRING,
        ad.description
    ) AS description,
    
    -- Followers
    fe.follower_emails AS followers,
    
    -- Assets
    an.names AS assets,

    -- Top Queries
    tq.queries AS top_queries
FROM
    NamespacesParsed np
    LEFT JOIN TagsAggregated ta ON np.id = ta.d_id
    LEFT JOIN FollowersWithEmails fe ON np.id = fe.d_id
    LEFT JOIN AssetContactInfo aci ON np.id = aci.d_id
    LEFT JOIN CreatedByUser cbu ON np.id = cbu.id
    LEFT JOIN AssetNames an ON np.id = an.id
    LEFT JOIN AssetDescription ad ON np.id = ad.id
    LEFT JOIN TopQueries tq ON np.id = tq.id
WHERE
    (np.AssetNamespace_info_json:detail:type::STRING = 'DATA_GROUP' 
    OR np.AssetNamespace_info_json:detail:type::STRING = 'DATA_UNIT')
    AND np.deleted_at IS NULL