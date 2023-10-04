package com.labelbox.adv.retrofill.rest

import com.labelbox.adv.retrofill.batching.DataRowLevelMigrationBatchJobConfig
import com.labelbox.adv.retrofill.batching.DatasetLevelMigrationBatchJobConfig
import com.labelbox.adv.retrofill.batching.OrgLevelMigrationBatchJobConfig
import com.labelbox.adv.retrofill.domain.MigrationDataRowRequest
import com.labelbox.adv.retrofill.domain.MigrationDataSetRequest
import com.labelbox.adv.retrofill.domain.MigrationOrgsRequest
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobExecutionNotRunningException
import org.springframework.batch.core.launch.NoSuchJobExecutionException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.ResponseBody
import org.springframework.web.bind.annotation.RestController

@RestController
class RetrofillController(
    val orgsMigrationBatchJobConfig: OrgLevelMigrationBatchJobConfig,
    val datasetMigrationBatchJobConfig: DatasetLevelMigrationBatchJobConfig,
    val dataRowMigrationBatchJobConfig: DataRowLevelMigrationBatchJobConfig
) {
    @PostMapping("/_migrate_organizations")
    @ResponseBody
    fun startOrgsigration(@RequestBody req: MigrationOrgsRequest): ResponseEntity<String> {
        if (orgsMigrationBatchJobConfig.getNumRunningJobs() + req.orgIds.size > ORG_JOB_LIMIT) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("Running Job limit $ORG_JOB_LIMIT is reached.")
        }

        if (req.orgIds.size > 1 && req.dataRowCheckpoint.length > 1) {
            return ResponseEntity.badRequest().body(String.format("Checkpoint should not be used against multiple orgs"))
        }

        if (!req.skipDataRow && req.skipAttachment) {
            return ResponseEntity.badRequest().body(String.format("It doesn't allow to skip attachment without skipping dataRow"))
        }

        if (req.splitExecution) {
            val jobIdToOrgMap = mutableMapOf<Long, String>()
            for (orgId in req.orgIds) {
                val jobParameters = JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .addString(ORG_IDS_PARAM, orgId)
                    .addString(SKIP_LD_SETTING, req.skipLdSetting.toString())
                    .addString(SKIP_DATAROW, req.skipDataRow.toString())
                    .addString(SKIP_ATTR, req.skipAttachment.toString())
                    .addString(TEST_RUN, req.isTestRun.toString())
                    .addString(DATAROW_CHECKPOINT, req.dataRowCheckpoint)
                    .addString(INC_UPDATE, req.enableIncrementalUpdate.toString())
                    .addLong(INC_UPDATE_MINS, req.incUpdateWindow)
                    .toJobParameters()
                val je = orgsMigrationBatchJobConfig.executeJob(jobParameters)
                jobIdToOrgMap[je.jobId] = orgId
            }
            return ResponseEntity.ok(
                String.format(
                    "Orgs migration is split per org," +
                        " job id to org id map: %s",
                    jobIdToOrgMap
                )
            )
        } else {
            val jobParameters = JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .addString(ORG_IDS_PARAM, req.orgIds.joinToString(","))
                .addString(SKIP_LD_SETTING, req.skipLdSetting.toString())
                .addString(SKIP_DATAROW, req.skipDataRow.toString())
                .addString(SKIP_ATTR, req.skipAttachment.toString())
                .addString(TEST_RUN, req.isTestRun.toString())
                .addString(DATAROW_CHECKPOINT, req.dataRowCheckpoint)
                .addString(INC_UPDATE, req.enableIncrementalUpdate.toString())
                .addLong(INC_UPDATE_MINS, req.incUpdateWindow)
                .toJobParameters()
            val je = orgsMigrationBatchJobConfig.executeJob(jobParameters)
            return ResponseEntity.ok(String.format("Orgs migration job started with job id: %d", je.jobId))
        }
    }

    @PostMapping("/_migrate_dataset")
    @ResponseBody
    fun startDatasetMigration(@RequestBody req: MigrationDataSetRequest): ResponseEntity<String> {
        if (datasetMigrationBatchJobConfig.getNumRunningJobs() + req.datasetIds.size > DATASET_JOB_LIMIT) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body("Running Job limit $DATASET_JOB_LIMIT is reached.")
        }

        if (req.datasetIds.size > 1 && req.dataRowCheckpoint.length > 1) {
            return ResponseEntity.badRequest().body(String.format("Checkpoint should not be used against multiple datasets"))
        }

        if (!req.skipDataRow && req.skipAttachment) {
            return ResponseEntity.badRequest().body(String.format("It doesn't allow to skip attachment without skipping dataRow"))
        }

        val jobParameters = JobParametersBuilder()
            .addLong("time", System.currentTimeMillis())
            .addString(DATASET_IDS_PARAM, req.datasetIds.joinToString(","))
            .addString(SKIP_DATAROW, req.skipDataRow.toString())
            .addString(SKIP_ATTR, req.skipAttachment.toString())
            .addString(DATAROW_CHECKPOINT, req.dataRowCheckpoint)
            .toJobParameters()
        val je = datasetMigrationBatchJobConfig.executeJob(jobParameters)
        return ResponseEntity.ok(String.format("Dataset migration job started with job id: %d", je.jobId))
    }

    @PostMapping("/_migrate_datarow")
    @ResponseBody
    fun startDataRowMigration(@RequestBody req: MigrationDataRowRequest): ResponseEntity<String> {
        val jobParameters = JobParametersBuilder()
            .addLong("time", System.currentTimeMillis())
            .addString(DATAROW_IDS_PARAM, req.dataRowIds.joinToString(","))
            .addString(TEST_RUN, req.isTestRun.toString())
            .toJobParameters()
        val je = dataRowMigrationBatchJobConfig.executeJob(jobParameters)
        return ResponseEntity.ok(String.format("DataRow migration job started with job id: %d", je.jobId))
    }

    @PostMapping("/_stop_migration_job/{jobExecutionId}")
    @ResponseBody
    fun stopDatasetMigrationJob(@PathVariable("jobExecutionId") jobExecutionId: Long): ResponseEntity<String> {
        return try {
            orgsMigrationBatchJobConfig.jobOperator().stop(jobExecutionId)
            ResponseEntity.ok(String.format("Adv migration job with id %d stopped", jobExecutionId))
        } catch (e: JobExecutionNotRunningException) {
            ResponseEntity.badRequest().body(String.format("Job execution with id %d is not running", jobExecutionId))
        } catch (e: NoSuchJobExecutionException) {
            ResponseEntity.badRequest().body(String.format("Job execution with id %d not found", jobExecutionId))
        }
    }

    @PostMapping("/_check_migration_job/{jobExecutionId}")
    @ResponseBody
    fun checkDatasetMigrationJob(@PathVariable("jobExecutionId") jobExecutionId: Long): ResponseEntity<String> {
        return try {
            // jobOperator shares the same H2 datasource
            val summary = orgsMigrationBatchJobConfig.jobOperator().getStepExecutionSummaries(jobExecutionId)
            val jobStatus = orgsMigrationBatchJobConfig.jobExplorer().getJobExecution(jobExecutionId)?.status.toString()
            ResponseEntity.ok(String.format("Job status is %s\nJob summary is %s", jobStatus, summary))
        } catch (e: NoSuchJobExecutionException) {
            ResponseEntity.badRequest().body(String.format("Job execution with id %d not found", jobExecutionId))
        }
    }

    companion object {
        val ORG_IDS_PARAM = "orgIds"
        val DATASET_IDS_PARAM = "datasetIds"
        val SKIP_DATAROW = "skipDataRow"
        val SKIP_ATTR = "skipAttachment"
        val ORG_JOB_LIMIT = 500
        val DATASET_JOB_LIMIT = 1000
        val SKIP_LD_SETTING = "skipLdSetting"
        val TEST_RUN = "testRun"
        val DATAROW_IDS_PARAM = "dataRowIds"
        val DATAROW_CHECKPOINT = "dataRowCheckPoint"
        val INC_UPDATE = "enableIncUpdate"
        val INC_UPDATE_MINS = "incUpdateMins"
    }
    // TODO: Add Endpoints for migrating dataRow.
}
