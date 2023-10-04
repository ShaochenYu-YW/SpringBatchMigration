package com.labelbox.adv.retrofill.dao

import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.DATASET_IDS_PARAM
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.SKIP_ATTR
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.SKIP_DATAROW
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class DatasetOnSameOrgDetector : JobExecutionDecider {

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    override fun decide(jobExecution: JobExecution, stepExecution: StepExecution?): FlowExecutionStatus {
        val datasetIds = jobExecution.jobParameters.getString(DATASET_IDS_PARAM)
        if (datasetIds == null) {
            throw RuntimeException("Missing datasetIds in the jobParameter!")
        } else {
            val idsList = datasetIds.split(",")
            val bindingVars = idsList.joinToString(",") { "?" }
            val query = """
                SELECT DISTINCT organizationId
                FROM Dataset 
                WHERE id IN ($bindingVars)
                and deleted = 0
                """
            val idArgs = idsList.toTypedArray()
            val countList = jdbcTemplate.queryForList(query, String::class.java, *idArgs)

            return if (countList.isEmpty()) {
                FlowExecutionStatus(INVALID_DATASET)
            } else {
                if (countList.size > 1) {
                    FlowExecutionStatus(MORE_THAN_ONE_ORGS)
                } else {
                    val skipDr = jobExecution.jobParameters.getString(SKIP_DATAROW).toBoolean()
                    val skipAttr = jobExecution.jobParameters.getString(SKIP_ATTR).toBoolean()
                    if (!skipDr && !skipAttr) {
                        FlowExecutionStatus(FULL_MIGRATION)
                    } else {
                        if (skipDr && !skipAttr) {
                            FlowExecutionStatus(PARTIAL_MIGRATION)
                        } else {
                            FlowExecutionStatus(PARTIAL_WITHOUT_ATTR_MIGRATION)
                        }
                    }
                }
            }
        }
    }

    companion object {
        val INVALID_DATASET = "INVALID_DATASET"
        val MORE_THAN_ONE_ORGS = "MORE_THAN_ONE_ORGS"
        val FULL_MIGRATION = "FULL_MIGRATION"
        val PARTIAL_MIGRATION = "PARTIAL_MIGRATION"
        val PARTIAL_WITHOUT_ATTR_MIGRATION = "PARTIAL_WITHOUT_ATTR_MIGRATION"
    }
}
