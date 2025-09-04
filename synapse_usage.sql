// petabytes uploaded over time (Figure 1)
WITH year_storage AS (
  SELECT
    DATE_TRUNC('YEAR', created_on) AS created_on_year,
    SUM(content_size) / power(2, 50) AS size_in_pib
  FROM
    synapse_data_warehouse.synapse.file_latest
  GROUP BY
    created_on_year
)
SELECT
  YEAR(created_on_year) as created_on_year,
  SUM(size_in_pib) OVER (ORDER BY created_on_year) AS total_storage_pib
FROM
  year_storage
ORDER BY
  created_on_year ASC;


// Number of unique Synapse account holders 2012-2024 (Figure 5)
with user_created_on as (
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
)
select
    created_on_yr,
    sum(number_of_users_created_accounts) over (order by created_on_yr asc rows between unbounded preceding and current row) as number_of_users
from
    user_created_on;


// number of downloads
// In 2024, there were 43,723,727 download events across 5,202,243 Synapse files, an average of 119,791 file download events per day
select
    count(*) as number_of_downloads,
    count(distinct file_handle_id) as number_of_files,
    number_of_downloads/365 as avg_downloads_per_day
from
    synapse_data_warehouse.synapse_event.objectdownload_event
where
    YEAR(record_date)= 2024 and
    association_object_id is not null and
    association_object_type IS NOT NULL and
    // TODO update this to only be files?
    project_id IS NOT NULL;

// Total number of files in Synapse as of 08/01/2025
with node_latest_before_2025_08_01 as (
    SELECT
        *
    FROM
        synapse_data_warehouse.synapse_event.node_event
    WHERE
        snapshot_timestamp >= date('2025-08-01') - interval '30 day'
        and snapshot_timestamp < date('2025-08-01')
    QUALIFY row_number() over (
        partition by id
        order by change_timestamp desc, snapshot_timestamp desc
    ) = 1
)
SELECT
    count(*) AS total_files
FROM
    node_latest_before_2025_08_01
WHERE NOT (
    change_type = 'DELETE'
    OR benefactor_id = '1681355'
    OR parent_id = '1681355'
);

select
    count(*)
from
    synapse_data_warehouse.synapse.node_latest;

// Synapse’s data governance infrastructure section
// number of is_restricted / is_controlled / is_public combinations
with all_nodes_before_date as (
    select
        id, is_public, is_restricted, is_controlled, CHANGE_TYPE, BENEFACTOR_ID, parent_id
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < DATE('2025-08-21') and
        node_type = 'file' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY modified_on DESC, snapshot_timestamp DESC
    ) = 1
), all_non_deleted_nodes_before_date as (
    select
        *
    from 
        all_nodes_before_date
    WHERE
        NOT (CHANGE_TYPE = 'DELETE' OR BENEFACTOR_ID = '1681355' OR PARENT_ID = '1681355') -- 1681355 is 
)
select
    COUNT(*) AS total_files,
    SUM(CASE WHEN is_public THEN 1 ELSE 0 END) AS num_public_files,
    SUM(CASE WHEN is_public and is_controlled and not is_restricted THEN 1 ELSE 0 END) AS num_public_and_only_controlled,
    SUM(CASE WHEN is_public and is_restricted and not is_controlled THEN 1 ELSE 0 END) AS num_public_and_only_clickwrap, 
    SUM(CASE WHEN is_public and is_restricted and is_controlled THEN 1 ELSE 0 END) AS num_public_and_controlled_and_clickwrap, 
    SUM(CASE WHEN is_public and is_controlled THEN 1 ELSE 0 END) AS num_public_and_controlled,
    SUM(CASE WHEN is_public and is_restricted THEN 1 ELSE 0 END) AS num_public_and_clickwrap,
from
    all_non_deleted_nodes_before_date;

    
// Table 4: storage volume before 08/01/2025

// ADTR
with adtr_snapshot_in_time as (
    select
        *
    from
        synapse_data_warehouse.synapse_event.node_event
    where
        modified_on < DATE('2025-09-04')
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY modified_on DESC, snapshot_timestamp DESC
        ) = 1
), adtr_latest as (
    select
        *
    from
        adtr_snapshot_in_time
    WHERE
        project_id = 2580853 and
        NOT (adtr_snapshot_in_time.CHANGE_TYPE = 'DELETE' OR adtr_snapshot_in_time.BENEFACTOR_ID = '1681355' OR adtr_snapshot_in_time.PARENT_ID = '1681355') -- 1681355 is the synID of the trash can on Synapse
)
SELECT
    count(distinct adtr_latest.id) as total_entities,
    count(distinct FILE_LATEST.ID) as TOTAL_FILES,
    round(sum(FILE_LATEST.CONTENT_SIZE) / power(2, 40), 2) AS TOTAL_SIZE_IN_TB
FROM
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.FILE_LATEST
join
    adtr_latest
    on file_latest.id = adtr_latest.file_handle_id;
// NF-OSI
// CCKP
// dHealth
// ARK
// Project GENIE
// ELITE


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
    association_object_type IS NOT NULL and
    project_id = 2580853 and 
    record_date < DATE('2025-08-01');

// NF-OSI
// CCKP
// dHealth
// ARK
// Project GENIE
// ELITE



// Upload csv from get_access_requirements.py
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
FROM sage.scidata.access_approvals;

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
    count(*) as number_of_access_approvals
    -- modifiedon - submittedon as timetoresponse
FROM
    SAGE.SCIDATA.ACCESS_APPROVALS
JOIN
    SYNAPSE_DATA_WAREHOUSE.SYNAPSE.ACCESSREQUIREMENT_LATEST
    ON ACCESS_APPROVALS.ACCESSREQUIREMENTID = ACCESSREQUIREMENT_LATEST.ID
group by
    ACCESSREQUIREMENT_LATEST.NAME;


// unused metrics
// Looking at chat users (not used in paper)
select
    count(distinct(user_id))
from
    synapse_data_warehouse.synapse_event.access_event
where
    normalized_method_signature = 'POST /agent/chat/async/start' and
    record_date > DATE('2025-01-01');
