package com.labelbox.adv.retrofill.dao

import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.DATAROW_IDS_PARAM
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class DataRowOnSameOrgDetector : JobExecutionDecider {

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    override fun decide(jobExecution: JobExecution, stepExecution: StepExecution?): FlowExecutionStatus {
        val dataRowIds = jobExecution.jobParameters.getString(DATAROW_IDS_PARAM)
        if (dataRowIds == null) {
            throw RuntimeException("Missing dataRowIds in the jobParameter!")
        } else {
            val idsList = dataRowIds.split(",")
            val bindingVars = idsList.joinToString(",") { "?" }
            val query = """
                SELECT DISTINCT organizationId
                FROM DataRow 
                WHERE id IN ($bindingVars)
                and deletedAt is NULL
                """
            val idArgs = idsList.toTypedArray()
            val countList = jdbcTemplate.queryForList(query, String::class.java, *idArgs)

            return if (countList.isEmpty()) {
                FlowExecutionStatus(INVALID_DATAROWS)
            } else {
                if (countList.size > 1) {
                    FlowExecutionStatus(MORE_THAN_ONE_ORGS)
                } else {
                    FlowExecutionStatus(PASS_DETECTION)
                }
            }
        }
    }

    companion object {
        val INVALID_DATAROWS = "INVALID_DATAROWS"
        val MORE_THAN_ONE_ORGS = "MORE_THAN_ONE_ORGS"
        val PASS_DETECTION = "PASS_DETECTION"
    }
}
