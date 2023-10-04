# SpringBatchMigration
Repository Description: MySQL to Spanner Data Migration Example

1. Overview & Context:
This repository offers a proven solution for migrating data from MySQL to Spanner. The MySQL database in the provided example includes four tables: DataRow, DataSet, Attachment, and Metadata. The equivalent tables in Spanner are DataRow, Dataset, Attachment, and DataRowMetadata, with roughly matching schemas.

Key highlights of the solution include:

Granularity for various business logic, such as Organization, Dataset, or DataRow.

Efficient data transformation capabilities during migration.

Flexible control over starting, checking, resuming, or canceling migration jobs.

Detailed migration reports, including data read and write statistics.

Support for incremental data updates during migration (via ElasticSearch source).

2. Getting Started:
To get started, utilize the following APIs:

_migrate_organizations
_migrate_dataset
_migrate_datarow
_stop_migration_job
_check_migration_job
Upon completion of a job (whether successful or failed), a webhook notification will be sent through Slack containing the migration report.
