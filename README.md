# synapse-paper
This repository contains the code executed for the Synapse.org paper

## `synapse_usage.sql`

This SQL script was used to generate the Synapse usage and storage statistics for the manuscript. To execute the SQL script, you must be a Sage Bionetworks employee with access to our internal Snowflake data warehouse.

## `get_access_requirements.py`

This Python code can only be executed by Sage Bionetworks employees with ACT permissions (governance team). The code extracts access approval information across 13 access requirements that span 4 DCCs, and generates a CSV file with the results. This CSV can then be uploaded into Snowflake to join with the rest of the Sage data warehouse.
