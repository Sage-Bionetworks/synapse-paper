// petabytes uploaded to synapse before 08/01/2025
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
        -- UPLOADED BEFORE 08/01/2025
        CREATED_ON < DATE('2025-08-01')
    GROUP BY
        S3_URL, CONTENT_MD5
    ORDER BY
        number_of_files DESC
)
select
    SUM(CONTENT_SIZE) / power(10, 15) AS size_in_pb
from
    filtered_files;

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
        AND CREATED_ON < DATE('2025-08-01')
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


// Number of synapse accounts created per year 2012-2024 (Figure 5)
select
    YEAR(created_on) as created_on_yr,
    count(*) as number_of_users_created_accounts
from
    synapse_data_warehouse.synapse.userprofile_latest
where
    created_on is not null and
    created_on_yr < 2025
group by
    created_on_yr



// number of downloads
// In 2024, there were 43,723,727 download events across 5,202,243 Synapse files, an average of 119,791 file download events per day
select
    count(*) as number_of_downloads,
    count(distinct file_handle_id) as number_of_files,
    count(distinct user_id) as number_of_unique_users,
    number_of_downloads/365 as avg_downloads_per_day
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    YEAR(record_date)= 2024 and
    association_object_id is not null and
    association_object_type IS NOT NULL and
    // TODO update this to only be files?
    project_id IS NOT NULL;

// In 2024, there were 43,510,660 download events across 5,029,700 Synapse files, an average of 119,207 file download events per day
// Only include file entity downloads
select
    count(*) as number_of_downloads,
    count(distinct file_handle_id) as number_of_files,
    count(distinct user_id) as number_of_unique_users,
    number_of_downloads/365 as avg_downloads_per_day
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    YEAR(record_date)= 2024 and
    association_object_id is not null and
    association_object_type = 'FileEntity' and
    // TODO update this to only be files?
    project_id IS NOT NULL;

// Total number of files in Synapse as of 08/01/2025
with node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
)
select
    count(*)
from
    node_latest_before_2025_08_01
where not (
    change_type = 'DELETE'
    or benefactor_id = '1681355'
    or parent_id     = '1681355'
);

// Synapse’s data governance infrastructure section
// number of is_restricted / is_controlled / is_public combinations

with node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), all_non_deleted_nodes_before_date as (
    select
        *
    from
        node_latest_before_2025_08_01
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
    SUM(CASE WHEN is_public and (is_controlled or is_restricted) THEN 1 ELSE 0 END) AS num_public_and_controlled_access,
    SUM(CASE WHEN is_public and is_restricted and is_controlled THEN 1 ELSE 0 END) AS num_public_and_controlled_and_clickwrap, 
    SUM(CASE WHEN is_public and is_controlled THEN 1 ELSE 0 END) AS num_public_and_controlled,
    SUM(CASE WHEN is_public and is_restricted THEN 1 ELSE 0 END) AS num_public_and_clickwrap,
from
    all_non_deleted_nodes_before_date;

    
// Table 4: storage volume before 08/01/2025
// ADTR

with node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), adtr_latest as (
    select
        *
    from
        node_latest_before_2025_08_01
    WHERE
        project_id = 2580853 and
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
)
SELECT
    count(distinct adtr_latest.id) as total_entities,
    count(distinct FILE_LATEST.ID) as TOTAL_FILES,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(2, 40), 2) AS TOTAL_SIZE_IN_TIB,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(10, 12), 2) AS TOTAL_SIZE_IN_TB
FROM
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
join
    adtr_latest
    on file_latest.id = adtr_latest.file_handle_id;
// NF-OSI

with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 52677631 and
        modified_on < DATE('2025-08-01')
    order by modified_on desc LIMIT 1
), nf_projects as (
    select
        cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes
), node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_2025_08_01
    join
        nf_projects
        on node_latest_before_2025_08_01.project_id = nf_projects.project_id
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
)
SELECT
    count(distinct project_latest.id) as total_entities,
    count(distinct FILE_LATEST.ID) as TOTAL_FILES,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(2, 40), 2) AS TOTAL_SIZE_IN_TIB,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(10, 12), 2) AS TOTAL_SIZE_IN_TB
FROM
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
join
    project_latest
    on file_latest.id = project_latest.file_handle_id;


// CCKP

with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 69808225 and
        modified_on < DATE('2025-09-14')
    order by modified_on desc LIMIT 1
), projects as (
    select
        distinct cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes
), node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_2025_08_01
    join
        projects
        on node_latest_before_2025_08_01.project_id = projects.project_id
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
)
SELECT
    count(distinct project_latest.id) as total_entities,
    count(distinct FILE_LATEST.ID) as TOTAL_FILES,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(2, 40), 2) AS TOTAL_SIZE_IN_TIB,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(10, 12), 2) AS TOTAL_SIZE_IN_TB
FROM
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
join
    project_latest
    on file_latest.id = project_latest.file_handle_id;

// dHealth
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 27210848 and
        modified_on < DATE('2025-08-01')
    order by modified_on desc LIMIT 1
), projects as (
    select
        cast(scopes.value as integer) as project_id
    from
        get_view_in_time,
        lateral flatten(input => get_view_in_time.scope_ids) scopes
), node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_2025_08_01
    join
        projects
        on node_latest_before_2025_08_01.project_id = projects.project_id
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
)
SELECT
    count(distinct project_latest.id) as total_entities,
    count(distinct FILE_LATEST.ID) as TOTAL_FILES,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(2, 40), 2) AS TOTAL_SIZE_IN_TIB,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(10, 12), 2) AS TOTAL_SIZE_IN_TB
FROM
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
join
    project_latest
    on file_latest.id = project_latest.file_handle_id;
// ARK
with node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_2025_08_01
    WHERE
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        ) and
        project_id in (
            26710600
        )
)
SELECT
    count(distinct project_latest.id) as total_entities,
    count(distinct FILE_LATEST.ID) as TOTAL_FILES,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(2, 40), 2) AS TOTAL_SIZE_IN_TIB,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(10, 12), 2) AS TOTAL_SIZE_IN_TB
FROM
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
join
    project_latest
    on file_latest.id = project_latest.file_handle_id;


// Project GENIE
with node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_2025_08_01
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
)
SELECT
    count(distinct project_latest.id) as total_entities,
    count(distinct FILE_LATEST.ID) as TOTAL_FILES,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(2, 30), 2) AS TOTAL_SIZE_IN_GIB,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(10, 9), 2) AS TOTAL_SIZE_IN_GB
FROM
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
join
    project_latest
    on file_latest.id = project_latest.file_handle_id;

// ELITE
with node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), elite_latest as (
    select
        *
    from
        node_latest_before_2025_08_01
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
)
SELECT
    count(distinct elite_latest.id) as total_entities,
    count(distinct FILE_LATEST.ID) as TOTAL_FILES,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(2, 40), 2) AS TOTAL_SIZE_IN_TIB,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(10, 12), 2) AS TOTAL_SIZE_IN_TB
FROM
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
join
    elite_latest
    on file_latest.id = elite_latest.file_handle_id;

// AMP-ALS

with node_latest_before_2025_08_01 as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < date('2025-08-01') and
        snapshot_date >= date('2025-08-01') - interval '30 days' and
        node_type = 'file'
    qualify row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
), project_latest as (
    select
        *
    from
        node_latest_before_2025_08_01
    WHERE
        project_id = 64892175 and
        NOT (
            CHANGE_TYPE = 'DELETE' OR
            BENEFACTOR_ID = '1681355' OR
            PARENT_ID = '1681355'
        )
)
SELECT
    count(distinct project_latest.id) as total_entities,
    count(distinct FILE_LATEST.ID) as TOTAL_FILES,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(2, 30), 2) AS TOTAL_SIZE_IN_GIB,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(10, 9), 2) AS TOTAL_SIZE_IN_GB
FROM
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
join
    project_latest
    on file_latest.id = project_latest.file_handle_id;

// Table 4: unique data downloaders 1/1/2022 - 7/31/2025
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
    record_date < DATE('2025-08-01') and
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
        modified_on < DATE('2025-08-01')
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
    record_date < DATE('2025-08-01');

// CCKP
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 69808225 and
        modified_on < DATE('2025-09-14')
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
    projects.*, node_latest.name
from
    synapse_data_warehouse.synapse.node_latest
right join
    projects on
    node_latest.id = projects.project_id;


with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 69808225 and
        modified_on < DATE('2025-09-14')
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
    record_date < DATE('2025-08-01');

// dHealth
with get_view_in_time as (
    // Get the view of the project as of a certain date.
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        id = 27210848 and
        modified_on < DATE('2025-08-01')
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
        modified_on < DATE('2025-08-01')
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
    record_date < DATE('2025-08-01');

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
    record_date < DATE('2025-08-01') and
    project_id in (
        26710600
    );
select
    count(distinct user_id) as number_of_unique_users,
    count(*) as number_of_downloads,
    min(record_date),
    max(record_date)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    association_object_id is not null and
    association_object_type is not null and
    record_date < DATE('2025-08-01') and
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
    record_date < DATE('2025-08-01') and
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
    record_date < DATE('2025-08-01') and
    project_id in (
        27229419,
        52072575,
        52072939,
        52237024,
        52642213,
        53124793
    );



// Upload csv fromget_access_requirements.py
// access approval distribution
select
    state,
    count(*) as number_of_access_approvals
from
    sage.scidata.access_approvals
group by
    state;

// Number of renewal submissions
SELECT
    SUM(
        CASE
            WHEN access_approvals.isrenewalsubmission THEN 1
            ELSE 0
        END
    ) AS total_true
fromsage.scidata.access_approvals;

// Detailed access approval table
SELECT
    ACCESSREQUIREMENT_LATEST.NAME,
    ACCESSREQUIREMENT_LATEST.CONCRETE_TYPE,
    USERPROFILE_LATEST.USER_NAME,
    ARRAY_SIZE(ACCESSORCHANGES) AS NUMBEROFREQUESTERS,
    ACCESS_APPROVALS.*,
    ACCESS_APPROVALS.researchProjectSnapshot:institution
FROM
    SAGE.SCIDATA.ACCESS_APPROVALS
JOIN
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.USERPROFILE_LATEST
    ON ACCESS_APPROVALS.SUBMITTEDBY = USERPROFILE_LATEST.ID
JOIN
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.ACCESSREQUIREMENT_LATEST
    ON ACCESS_APPROVALS.ACCESSREQUIREMENTID = ACCESSREQUIREMENT_LATEST.ID;

// Institution distribution of access approvals
select
    ACCESS_APPROVALS.researchProjectSnapshot:institution as institution,
    count(*) as number_of_access_approvals
from
    SAGE.SCIDATA.ACCESS_APPROVALS
group by
    institution;

// Number of access approvals by access requirement
SELECT
    ACCESSREQUIREMENT_LATEST.NAME,
    ACCESSREQUIREMENT_LATEST.ID,
    count(*) as number_of_access_approvals
    -- modifiedon - submittedon as timetoresponse
FROM
    SAGE.SCIDATA.ACCESS_APPROVALS
JOIN
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.ACCESSREQUIREMENT_LATEST
    ON ACCESS_APPROVALS.ACCESSREQUIREMENTID = ACCESSREQUIREMENT_LATEST.ID
group by
    ACCESSREQUIREMENT_LATEST.ID,
    ACCESSREQUIREMENT_LATEST.NAME;

select
    ID, NAME
from
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.ACCESSREQUIREMENT_LATEST
where
    id in (
        9606614,
        9606610,
        9606593,
        9606557,
        9606091,
        9606506
    );

// UKB data users and volume Nov 1, 2023 - Nov 30, 2023
select
    count(distinct user_id)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    project_id = 51364943 and
    record_date BETWEEN DATE('2023-10-01') AND DATE('2023-11-30');

select
    sum(file_latest.content_size) / power(10, 12) as size_in_tb
from
    synapse_data_warehouse.synapse_event.objectdownload_event
join
    synapse_data_warehouse.synapse.file_latest
    on objectdownload_event.file_handle_id = file_latest.id
where
    project_id = 51364943 and
    record_date BETWEEN DATE('2023-10-01') AND DATE('2023-11-30');

// UKB data users and volume 2024
select
    count(distinct user_id)
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    project_id = 51364943 and
    record_date BETWEEN DATE('2024-01-01') AND DATE('2024-12-31');

select
    sum(file_latest.content_size) / power(10, 12) as size_in_tb
from
    synapse_data_warehouse.synapse_event.objectdownload_event
join
    synapse_data_warehouse.synapse.file_latest
    on objectdownload_event.file_handle_id = file_latest.id
where
    project_id = 51364943 and
    record_date BETWEEN DATE('2024-01-01') AND DATE('2024-12-31');

// unused metrics
// Looking at chat users (not used in paper)
select
    count(distinct(user_id))
from
    synapse_data_warehouse.synapse_event.access_event
where
    normalized_method_signature = 'POST /agent/chat/async/start' and
    record_date > DATE('2025-01-01');
