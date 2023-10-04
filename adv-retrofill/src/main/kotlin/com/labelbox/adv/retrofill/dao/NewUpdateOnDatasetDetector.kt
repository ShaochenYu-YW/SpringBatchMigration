package com.labelbox.adv.retrofill.dao

import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.DATASET_IDS_PARAM
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.time.format.DateTimeFormatter

@Component
class NewUpdateOnDatasetDetector : JobExecutionDecider {
    // This detector will be added for incremental update checking.

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    override fun decide(jobExecution: JobExecution, stepExecution: StepExecution?): FlowExecutionStatus {
        val datasetIds = jobExecution.jobParameters.getString(DATASET_IDS_PARAM)
        val ts = jobExecution.createTime
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        val tsString = ts.format(formatter)
        if (datasetIds == null) {
            throw RuntimeException("Missing datasetIds in the jobParameter!")
        } else {
            val idsList = datasetIds.split(",")
            val bindingVars = idsList.joinToString(",") { "?" }
            val query = """
                SELECT id
                FROM Dataset 
                WHERE id IN ($bindingVars)
                AND updatedAt > ?
                """
            val idArgs = idsList.toTypedArray()
            val idList = jdbcTemplate.queryForList(query, String::class.java, *idArgs, tsString)

            return if (idList.isEmpty()) {
                FlowExecutionStatus(PASS_DETECTION)
            } else {
                val idsStr = idList.joinToString(separator = ",")
                jobExecution.executionContext.putString(DATASET_IDS_TO_UPDATE, idsStr)
                FlowExecutionStatus(INCREMENTAL_UPDATE_NEEDED)
            }
        }
    }

    companion object {
        val INCREMENTAL_UPDATE_NEEDED = "INCREMENTAL_UPDATE_NEEDED"
        val PASS_DETECTION = "PASS_DETECTION"
        val DATASET_IDS_TO_UPDATE = "DATASET_IDS_TO_UPDATE"
    }
}
