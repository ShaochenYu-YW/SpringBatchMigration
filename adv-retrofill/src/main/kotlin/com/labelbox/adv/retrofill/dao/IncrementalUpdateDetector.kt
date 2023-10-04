package com.labelbox.adv.retrofill.dao

import com.labelbox.adv.retrofill.batching.DatasetLevelMigrationBatchJobConfig
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.DATASET_IDS_PARAM
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.ORG_IDS_PARAM
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.sql.Timestamp

@Component
class IncrementalUpdateDetector : JobExecutionDecider {

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    lateinit var datasetMigrationBatchJobConfig: DatasetLevelMigrationBatchJobConfig

    override fun decide(jobExecution: JobExecution, stepExecution: StepExecution?): FlowExecutionStatus {
        val orgIds = jobExecution.jobParameters.getString(ORG_IDS_PARAM)
        if (orgIds == null) {
            throw RuntimeException("Missing datasetIds in the jobParameter!")
        } else {
            jobExecution.startTime ?.let {
                val idsList = orgIds.split(",")
                val bindingVars = idsList.joinToString(",") { "?" }
                val query = """
                    SELECT 
                        id
                    FROM Dataset 
                    WHERE organizationId IN ($bindingVars)
                    and deleted = 0
                    and updatedAt > ?
                """
                val idArgs = idsList.toTypedArray()
                // Same default timezone as UTC on us-central1
                val timer = Timestamp.valueOf(it)

                val datasetIdList = jdbcTemplate.queryForList(query, String::class.java, *idArgs, timer)

                val incrementalJobs = mutableListOf<String>()

                for (i in 0 until datasetIdList.size step BATCH_SIZE) {
                    val datasetIdBatch = datasetIdList.subList(i, (i + BATCH_SIZE).coerceAtMost(datasetIdList.size))
                    val jobParameters = JobParametersBuilder()
                        .addLong("time", System.currentTimeMillis())
                        .addString(DATASET_IDS_PARAM, datasetIdBatch.joinToString(","))
                        .toJobParameters()
                    val je = datasetMigrationBatchJobConfig.executeJob(jobParameters)
                    incrementalJobs.add(je.jobId.toString())
                }

                if (incrementalJobs.isNotEmpty()) {
                    jobExecution.executionContext.put("incremental_jobs", incrementalJobs.joinToString(","))
                }
            }

            return FlowExecutionStatus(PASS_DETECTION)
        }
    }

    companion object {
        val PASS_DETECTION = "PASS_DETECTION"
        val BATCH_SIZE = 50
    }
}
