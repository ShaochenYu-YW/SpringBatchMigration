# SpringBatchMigration
This Repo Demonstrate an example of migrating MySQL datasource to Spanner.

1. Overview & Context
   It provides proven solution to migrate from MySQL to Spanner. The MySQL Database in the given example has 4 table as DataRow, DataSet, Attachment, Metadata. The corresponding tables in Spanner are respective as DataRow, Dataset, Attachment, DataRowMetadata. Their schema roughly matched.
   The major highlights for the solution:
   a. Granularity for different business logic, etc, for Organization, Dataset or DataRow.
   b. It support efficient data transformation during migration.
   c. Flexiblely start/check/resume/cancel migraiton job.
   d. Detailed migration resport which includes number of data read/write.
   e. Incremental update for data creation during migration(by ElasticSearch source)
2. Get Started
   It has following APIs:
   1. _migrate_organizations
   2. _migrate_dataset
   3. _migrate_datarow
   4. _stop_migration_job
   5. _check_migration_job
   Once the job is done(sucess or fail), it will send webhook through Slack for the migration report.
