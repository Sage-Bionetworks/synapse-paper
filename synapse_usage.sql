-- Synapse data warehouse analysis queries
-- Co-authored with CoCo
// ABSTRACT & INTRODUCTION
// Date anchor points
SET DATE_ANCHOR = DATE('2026-06-01');
SET YEAR_ANCHOR = YEAR($DATE_ANCHOR);
// Total users
select
    count(*) as number_of_users
from
    synapse_data_warehouse.synapse.userprofile_latest
where
    created_on < $DATE_ANCHOR;

// Total petabytes uploaded to synapse that live within our servers before DATE_ANCHOR
with filtered_files as (
    SELECT
        CONCAT(BUCKET, '/', KEY) as S3_URL,
        CONTENT_MD5,
        MAX(CONTENT_SIZE) as CONTENT_SIZE,
        count(*) as number_of_files
    FROM
        SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
    WHERE
        -- NOT STORED ON OUR SERVERS
        BUCKET IS NOT NULL
        -- AND
        --  -- NOT A DELETED FILE
        -- CHANGE_TYPE != 'DELETE'
        AND
        -- NOT MARKED FOR GARBAGE COLLECTION
        STATUS != 'ARCHIVED' AND NOT IS_PREVIEW AND
        -- UPLOADED BEFORE DATE_ANCHOR
        CREATED_ON < $DATE_ANCHOR
    GROUP BY
        S3_URL, CONTENT_MD5
    ORDER BY
        number_of_files DESC
)
select
    SUM(CONTENT_SIZE) / power(10, 15) AS size_in_pb
from
    filtered_files;

// ABSTRACT & INTRODUCTION
// petabytes uploaded over time (Figure 1)
WITH file_details AS (
    SELECT
        CONCAT(BUCKET, '/', KEY) AS s3_url,
        CONTENT_MD5,
        MAX(CONTENT_SIZE) AS content_size,
        min(DATE_TRUNC('YEAR', CREATED_ON)) as first_upload_year
    FROM
        SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
    WHERE
        BUCKET IS NOT NULL
        AND STATUS != 'ARCHIVED'
        AND NOT IS_PREVIEW
        AND CREATED_ON < $DATE_ANCHOR
    GROUP BY
        s3_url, CONTENT_MD5
)

SELECT
    first_upload_year,
    SUM(content_size) / power(10, 15) AS pb_uploaded_this_year,
    SUM(SUM(content_size) / power(10, 15) ) OVER (ORDER BY first_upload_year) AS cumulative_pb_uploaded
FROM
    file_details
GROUP BY
    first_upload_year
ORDER BY
    first_upload_year;


// 2.2 Synapse users and data use
// Number of synapse accounts created per year 2012-2024 (Table 2)
select
    YEAR(created_on) as created_on_yr,
    count(*) as number_of_users_created_accounts
from
    synapse_data_warehouse.synapse.userprofile_latest
where
    created_on is not null and
    created_on_yr < $YEAR_ANCHOR
group by
    created_on_yr
order by
    created_on_yr;

// 2.2 Synapse users and data use
// In YEAR_ANCHOR - 1, how many download events were there across how many Synapse files,
// What was the average file download events per day
// Only include file entity downloads
select
    count(*) as number_of_downloads,
    count(distinct file_handle_id) as number_of_files,
    count(distinct user_id) as number_of_unique_users,
    number_of_downloads/365 as avg_downloads_per_day
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    YEAR(record_date)= ($YEAR_ANCHOR-1) and
    association_object_id is not null and
    association_object_type = 'FileEntity' and
    project_id IS NOT NULL;

// 2.2 Synapse users and data use
// Total number of files in Synapse as of 08/01/2025
with node_latest_before_date_anchor as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < $DATE_ANCHOR and
        snapshot_date >= $DATE_ANCHOR - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
)
select
    count(*)
from
    node_latest_before_date_anchor
where not (
    change_type = 'DELETE'
    or benefactor_id = '1681355'
    or parent_id     = '1681355'
);

// 2.3 Synapse’s data governance infrastructure
// number of is_restricted / is_controlled / is_public combinations

with node_latest_before_date_anchor as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < $DATE_ANCHOR and
        snapshot_date >= $DATE_ANCHOR - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), all_non_deleted_nodes_before_date as (
    select
        *
    from
        node_latest_before_date_anchor
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
)
select
    COUNT(*) AS total_files,
    SUM(CASE WHEN is_public THEN 1 ELSE 0 END) AS num_public_files,
    SUM(CASE WHEN is_public and is_controlled and not is_restricted THEN 1 ELSE 0 END) AS num_public_and_only_controlled,
    SUM(CASE WHEN is_public and is_restricted and not is_controlled THEN 1 ELSE 0 END) AS num_public_and_only_clickwrap, 
    SUM(CASE WHEN is_public and (is_controlled or is_restricted) THEN 1 ELSE 0 END) AS num_public_and_controlled_or_clickwrap,
    SUM(CASE WHEN is_public and is_restricted and is_controlled THEN 1 ELSE 0 END) AS num_public_and_controlled_and_clickwrap, 
    SUM(CASE WHEN is_public and is_controlled THEN 1 ELSE 0 END) AS num_public_and_controlled,
    SUM(CASE WHEN is_public and is_restricted THEN 1 ELSE 0 END) AS num_public_and_clickwrap,
from
    all_non_deleted_nodes_before_date;

// 2.3 Synapse’s data governance infrastructure
// Upload csv from get_access_requirements.py into snowflake
// access approval distribution

select
    submission_status,
    count(*) as number_of_access_approvals
from
    sage.governance.data_access_submission_dashboard
where
    submitted_on < $DATE_ANCHOR and
    access_requirement_id in (
        9603055,
        9605913,
        9606644,
        9606593,
        9606557,
        9606541,
        9606508,
        9605435,
        9605422,
        9605255,
        9605240,
        9606270,
        9606268,
        9606115,
        9605543,
        9605351,
        9606506,
        9606091,
        9605444,
        9605700
    )
group by
    submission_status;

select
    state,
    count(*) as number_of_access_approvals
from
    sage.scidata.access_approvals
group by
    state;

-- // 2.3 Synapse’s data governance infrastructure
-- // Number of renewal submissions
-- SELECT
--     SUM(
--         CASE
--             WHEN access_approvals.isrenewalsubmission THEN 1
--             ELSE 0
--         END
--     ) AS total_true
-- fromsage.scidata.access_approvals;

-- // 2.3 Synapse’s data governance infrastructure
-- // Detailed access approval table
-- SELECT
--     ACCESSREQUIREMENT_LATEST.NAME,
--     ACCESSREQUIREMENT_LATEST.CONCRETE_TYPE,
--     USERPROFILE_LATEST.USER_NAME,
--     ARRAY_SIZE(ACCESSORCHANGES) AS NUMBEROFREQUESTERS,
--     ACCESS_APPROVALS.*,
--     ACCESS_APPROVALS.researchProjectSnapshot:institution
-- FROM
--     SAGE.SCIDATA.ACCESS_APPROVALS
-- JOIN
--     SYNAPSE_DATA_WAREHOUSE.SYNAPSE.USERPROFILE_LATEST
--     ON ACCESS_APPROVALS.SUBMITTEDBY = USERPROFILE_LATEST.ID
-- JOIN
--     SYNAPSE_DATA_WAREHOUSE.SYNAPSE.ACCESSREQUIREMENT_LATEST
--     ON ACCESS_APPROVALS.ACCESSREQUIREMENTID = ACCESSREQUIREMENT_LATEST.ID;

-- // 2.3 Synapse’s data governance infrastructure
-- // Institution distribution of access approvals
-- select
--     ACCESS_APPROVALS.researchProjectSnapshot:institution as institution,
--     count(*) as number_of_access_approvals
-- from
--     SAGE.SCIDATA.ACCESS_APPROVALS
-- group by
--     institution;

-- // 2.3 Synapse’s data governance infrastructure
-- // Number of access approvals by access requirement
-- SELECT
--     ACCESSREQUIREMENT_LATEST.NAME,
--     ACCESSREQUIREMENT_LATEST.ID,
--     count(*) as number_of_access_approvals
--     -- modifiedon - submittedon as timetoresponse
-- FROM
--     SAGE.SCIDATA.ACCESS_APPROVALS
-- JOIN
--     SYNAPSE_DATA_WAREHOUSE.SYNAPSE.ACCESSREQUIREMENT_LATEST
--     ON ACCESS_APPROVALS.ACCESSREQUIREMENTID = ACCESSREQUIREMENT_LATEST.ID
-- group by
--     ACCESSREQUIREMENT_LATEST.ID,
--     ACCESSREQUIREMENT_LATEST.NAME;

// 2.4.3 The cost of reuse: data egress
// UKB data users and volume Nov 1, 2023 - Nov 30, 2023
-- select
--     count(distinct user_id)
-- from
--     synapse_data_warehouse.synapse_event.objectdownload_event
-- where
--     project_id = 51364943 and
--     record_date BETWEEN DATE('2023-10-01') AND DATE('2023-11-30');

-- select
--     sum(file_latest.content_size) / power(10, 12) as size_in_tb
-- from
--     synapse_data_warehouse.synapse_event.objectdownload_event
-- join
--     synapse_data_warehouse.synapse.file_latest
--     on objectdownload_event.file_handle_id = file_latest.id
-- where
--     project_id = 51364943 and
--     record_date BETWEEN DATE('2023-10-01') AND DATE('2023-11-30');

-- // 2.4.3 The cost of reuse: data egress
-- // UKB data users and volume 2024
-- select
--     count(distinct user_id)
-- from
--     synapse_data_warehouse.synapse_event.objectdownload_event
-- where
--     project_id = 51364943 and
--     record_date BETWEEN DATE('2024-01-01') AND DATE('2024-12-31');

-- select
--     sum(file_latest.content_size) / power(10, 12) as size_in_tb
-- from
--     synapse_data_warehouse.synapse_event.objectdownload_event
-- join
--     synapse_data_warehouse.synapse.file_latest
--     on objectdownload_event.file_handle_id = file_latest.id
-- where
--     project_id = 51364943 and
--     record_date BETWEEN DATE('2024-01-01') AND DATE('2024-12-31');

// Interoperability via Programmatic Access
select
    CASE
        WHEN access_event.client in ('PYTHON', 'COMMAND_LINE', 'SYNAPSER') THEN 'API_CLIENT'
        ELSE access_event.client
    END client_agg,
    count(objectdownload_event.record_date) as number_of_downloads,
    count(distinct objectdownload_event.user_id) as number_of_users,
    sum(file_latest.content_size) / power(2, 40) as egress_tib
from
    synapse_data_warehouse.synapse_event.objectdownload_event
join
    synapse_data_warehouse.synapse_event.access_event on
    objectdownload_event.session_id = access_event.session_id
join
    synapse_data_warehouse.synapse.file_latest on
    objectdownload_event.file_handle_id = file_latest.id
where
    YEAR(objectdownload_event.record_date) = ($YEAR_ANCHOR - 1) and
    YEAR(access_event.record_date) = ($YEAR_ANCHOR - 1) and
    access_event.client not in ('JAVA', 'UNKNOWN')
group by
    client_agg;


// 2.5.3 Portals
// Table 4: storage volume before date anchor
// ADTR
with node_latest_before_date_anchor as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < $DATE_ANCHOR and
        snapshot_date >= $DATE_ANCHOR - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_date_anchor
    WHERE
        project_id = 2580853 and
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
), file_stats as (
    SELECT
        CONCAT(BUCKET, '/', KEY) as S3_URL,
        CONTENT_MD5,
        project_latest.is_public,
        max(file_latest.content_size) AS MAX_FILE_SIZE,
    FROM
        SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
    join
        project_latest
        on file_latest.id = project_latest.file_handle_id
    GROUP BY
        S3_URL, CONTENT_MD5, project_latest.is_public
)
select
    sum(max_file_size) / power(10, 12) as size_in_tb,
    SUM(CASE WHEN is_public THEN max_file_size ELSE 0 END) / POWER(10, 12) AS public_size_tb,
    (
        SELECT
            100.0 * SUM(
                CASE
                    WHEN project_latest.annotations:annotations != {} AND is_public
                    THEN 1
                    ELSE 0
                END
            )
            / NULLIF(SUM(CASE WHEN is_public THEN 1 ELSE 0 END), 0)
        FROM project_latest
    ) as percent_annotated
from
    file_stats;

// NF-OSI
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 52677631 and
        modified_on < $DATE_ANCHOR
    order by modified_on desc LIMIT 1
), nf_projects as (
    select
        cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes
), node_latest_before_date_anchor as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < $DATE_ANCHOR and
        snapshot_date >= $DATE_ANCHOR - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_date_anchor
    join
        nf_projects
        on node_latest_before_date_anchor.project_id = nf_projects.project_id
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
), file_stats as (
    SELECT
        CONCAT(BUCKET, '/', KEY) as S3_URL,
        CONTENT_MD5,
        project_latest.is_public,
        max(file_latest.content_size) AS MAX_FILE_SIZE,
    FROM
        SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
    join
        project_latest
        on file_latest.id = project_latest.file_handle_id
    GROUP BY
        S3_URL, CONTENT_MD5, project_latest.is_public
)
select
    sum(max_file_size) / power(10, 12) as size_in_tb,
    SUM(CASE WHEN is_public THEN max_file_size ELSE 0 END) / POWER(10, 12) AS public_size_tb,
    (
        SELECT
            100.0 * SUM(
                CASE
                    WHEN project_latest.annotations:annotations != {} AND is_public
                    THEN 1
                    ELSE 0
                END
            )
            / NULLIF(SUM(CASE WHEN is_public THEN 1 ELSE 0 END), 0)
        FROM project_latest
    ) as percent_annotated
from
    file_stats;


// CCKP
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 69808225 and
        modified_on < $DATE_ANCHOR
    order by modified_on desc LIMIT 1
), projects as (
    select
        distinct cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes
), node_latest_before_date_anchor as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < $DATE_ANCHOR and
        snapshot_date >= $DATE_ANCHOR - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_date_anchor
    join
        projects
        on node_latest_before_date_anchor.project_id = projects.project_id
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
), file_stats as (
    SELECT
        CONCAT(BUCKET, '/', KEY) as S3_URL,
        CONTENT_MD5,
        project_latest.is_public,
        max(file_latest.content_size) AS MAX_FILE_SIZE,
    FROM
        SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
    join
        project_latest
        on file_latest.id = project_latest.file_handle_id
    GROUP BY
        S3_URL, CONTENT_MD5, project_latest.is_public
)
select
    sum(max_file_size) / power(10, 12) as size_in_tb,
    SUM(CASE WHEN is_public THEN max_file_size ELSE 0 END) / POWER(10, 12) AS public_size_tb,
    (
        SELECT
            100.0 * SUM(
                CASE
                    WHEN project_latest.annotations:annotations != {} AND is_public
                    THEN 1
                    ELSE 0
                END
            )
            / NULLIF(SUM(CASE WHEN is_public THEN 1 ELSE 0 END), 0)
        FROM project_latest
    ) as percent_annotated
from
    file_stats;

// dHealth
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 27210848 and
        modified_on < $DATE_ANCHOR
    order by modified_on desc LIMIT 1
), projects as (
    select
        cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes
), node_latest_before_date_anchor as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < $DATE_ANCHOR and
        snapshot_date >= $DATE_ANCHOR - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_date_anchor
    join
        projects
        on node_latest_before_date_anchor.project_id = projects.project_id
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
), file_stats as (
    SELECT
        CONCAT(BUCKET, '/', KEY) as S3_URL,
        CONTENT_MD5,
        project_latest.is_public,
        max(file_latest.content_size) AS MAX_FILE_SIZE,
    FROM
        SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
    join
        project_latest
        on file_latest.id = project_latest.file_handle_id
    GROUP BY
        S3_URL, CONTENT_MD5, project_latest.is_public
)
select
    sum(max_file_size) / power(10, 12) as size_in_tb,
    SUM(CASE WHEN is_public THEN max_file_size ELSE 0 END) / POWER(10, 12) AS public_size_tb,
    (
        SELECT
            100.0 * SUM(
                CASE
                    WHEN project_latest.annotations:annotations != {} AND is_public
                    THEN 1
                    ELSE 0
                END
            )
            / NULLIF(SUM(CASE WHEN is_public THEN 1 ELSE 0 END), 0)
        FROM project_latest
    ) as percent_annotated
from
    file_stats;

// ARK
with node_latest_before_date_anchor as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < $DATE_ANCHOR and
        snapshot_date >= $DATE_ANCHOR - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_date_anchor
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        ) and
        project_id in (
            26710600
        )
), file_stats as (
    SELECT
        CONCAT(BUCKET, '/', KEY) as S3_URL,
        CONTENT_MD5,
        project_latest.is_public,
        max(file_latest.content_size) AS MAX_FILE_SIZE,
    FROM
        SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
    join
        project_latest
        on file_latest.id = project_latest.file_handle_id
    GROUP BY
        S3_URL, CONTENT_MD5, project_latest.is_public
)
select
    sum(max_file_size) / power(10, 12) as size_in_tb,
    SUM(CASE WHEN is_public THEN max_file_size ELSE 0 END) / POWER(10, 12) AS public_size_tb,
    (
        SELECT
            100.0 * SUM(
                CASE
                    WHEN project_latest.annotations:annotations != {} AND is_public
                    THEN 1
                    ELSE 0
                END
            )
            / NULLIF(SUM(CASE WHEN is_public THEN 1 ELSE 0 END), 0)
        FROM project_latest
    ) as percent_annotated
from
    file_stats;

// Project GENIE
with node_latest_before_date_anchor as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < $DATE_ANCHOR and
        snapshot_date >= $DATE_ANCHOR - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_date_anchor
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        ) and
        project_id in (
            7222066,
            27056172
        )
), file_stats as (
    SELECT
        CONCAT(BUCKET, '/', KEY) as S3_URL,
        CONTENT_MD5,
        project_latest.is_public,
        max(file_latest.content_size) AS MAX_FILE_SIZE,
    FROM
        SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
    join
        project_latest
        on file_latest.id = project_latest.file_handle_id
    GROUP BY
        S3_URL, CONTENT_MD5, project_latest.is_public
)
select
    sum(max_file_size) / power(10, 9) as size_in_gb,
    SUM(CASE WHEN is_public THEN max_file_size ELSE 0 END) / POWER(10, 9) AS public_size_gb,
    (
        SELECT
            100.0 * SUM(
                CASE
                    WHEN project_latest.annotations:annotations != {} AND is_public
                    THEN 1
                    ELSE 0
                END
            )
            / NULLIF(SUM(CASE WHEN is_public THEN 1 ELSE 0 END), 0)
        FROM project_latest
    ) as percent_annotated
from
    file_stats;

// ELITE
with node_latest_before_date_anchor as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < $DATE_ANCHOR and
        snapshot_date >= $DATE_ANCHOR - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_date_anchor
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        ) and
        project_id in (
            27229419,
            52072575,
            52072939,
            52237024,
            52642213,
            53124793
        )
), file_stats as (
    SELECT
        CONCAT(BUCKET, '/', KEY) as S3_URL,
        CONTENT_MD5,
        project_latest.is_public,
        max(file_latest.content_size) AS MAX_FILE_SIZE,
    FROM
        SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
    join
        project_latest
        on file_latest.id = project_latest.file_handle_id
    GROUP BY
        S3_URL, CONTENT_MD5, project_latest.is_public
)
select
    sum(max_file_size) / power(10, 12) as size_in_tb,
    SUM(CASE WHEN is_public THEN max_file_size ELSE 0 END) / POWER(10, 12) AS public_size_tb,
    (
        SELECT
            100.0 * SUM(
                CASE
                    WHEN project_latest.annotations:annotations != {} AND is_public
                    THEN 1
                    ELSE 0
                END
            )
            / NULLIF(SUM(CASE WHEN is_public THEN 1 ELSE 0 END), 0)
        FROM project_latest
    ) as percent_annotated
from
    file_stats;

// 2.5.3 Portals
// Table 4: unique data downloaders 1/1/2022 - date_anchor
// ADTR
select
    count(distinct user_id),
    min(record_date),
    max(record_date)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    association_object_id is not null and
    association_object_type = 'FileEntity' and
    record_date < $DATE_ANCHOR and
    project_id = 2580853;

// NF-OSI
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 52677631 and
        modified_on < $DATE_ANCHOR
    order by modified_on desc LIMIT 1
), nf_projects as (
    select
        cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes
)
select
    count(distinct user_id),
    min(record_date),
    max(record_date)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
join
    nf_projects on
    nf_projects.project_id = objectdownload_event.project_id
where
    association_object_id is not null and
    -- association_object_type is not null and
    association_object_type = 'FileEntity' and
    record_date < $DATE_ANCHOR;

// CCKP
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 69808225 and
        modified_on < $DATE_ANCHOR
    order by modified_on desc LIMIT 1
    -- The view was created on 09/12/2025, but the projects within this project view were created before 2025
), projects as (
    select
        distinct cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes
)
select
    count(distinct user_id),
    min(record_date),
    max(record_date)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
join
    projects on
    projects.project_id = objectdownload_event.project_id
where
    association_object_id is not null and
    -- association_object_type is not null and
    association_object_type = 'FileEntity' and
    record_date < $DATE_ANCHOR;

// dHealth
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 27210848 and
        modified_on < $DATE_ANCHOR
    order by modified_on desc LIMIT 1
)
    select
        cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes;
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 21994970 and
        modified_on < $DATE_ANCHOR
    order by modified_on desc LIMIT 1
), projects as (
    select
        cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes
)
select
    count(distinct user_id),
    min(record_date),
    max(record_date)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
join
    projects on
    projects.project_id = objectdownload_event.project_id
where
    association_object_id is not null and
    -- association_object_type is not null and
    association_object_type = 'FileEntity' and
    record_date < $DATE_ANCHOR;

// ARK
select
    count(distinct user_id) as number_of_unique_users,
    count(*) as number_of_downloads,
    min(record_date),
    max(record_date)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    association_object_id is not null and
    association_object_type = 'FileEntity' and
    record_date < $DATE_ANCHOR and
    project_id in (
        26710600
    );

// Project GENIE
select
    count(distinct user_id) as number_of_unique_users,
    count(*) as number_of_downloads,
    min(record_date),
    max(record_date)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    association_object_id is not null and
    association_object_type = 'FileEntity' and
    record_date < $DATE_ANCHOR and
    project_id in (
        7222066,
        27056172
    );

// ELITE
select
    count(distinct user_id) as number_of_unique_users,
    count(*) as number_of_downloads,
    min(record_date),
    max(record_date)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    association_object_id is not null and
    association_object_type = 'FileEntity' and
    record_date < $DATE_ANCHOR and
    project_id in (
        27229419,
        52072575,
        52072939,
        52237024,
        52642213,
        53124793
    );
