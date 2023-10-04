package com.labelbox.adv.retrofill.dao

import com.labelbox.adv.retrofill.rest.RetrofillController
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class DanglingDataRowDetector : JobExecutionDecider {

    private val jobIdsName = "orgIds"

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    override fun decide(jobExecution: JobExecution, stepExecution: StepExecution?): FlowExecutionStatus {
        val orgIds = jobExecution.jobParameters.getString(jobIdsName)
        if (orgIds == null) {
            throw RuntimeException("Missing orgIds in the jobParameter!")
        } else {
            val orgidsList = orgIds.split(",")
            val bindingVars = orgidsList.joinToString(",") { "?" }
            val query = """
                SELECT 1
                FROM DataRow DR
                INNER JOIN Dataset DS ON DR.datasetId = DS.id
                WHERE DS.organizationId IN ($bindingVars)
                  AND DS.deleted = 1
                  AND DR.organizationId IN ($bindingVars)
                  AND DR.deletedAt IS NULL
                """
            val orgIdArgs = orgidsList.toTypedArray()
            val countList = jdbcTemplate.queryForList(query, Int::class.java, *orgIdArgs, *orgIdArgs)

            val executionTuple =
                Triple(
                    countList.isEmpty(),
                    Pair(
                        jobExecution.jobParameters.getString(RetrofillController.SKIP_DATAROW).toBoolean(),
                        jobExecution.jobParameters.getString(RetrofillController.SKIP_ATTR).toBoolean()
                    ),
                    jobExecution.jobParameters.getString(RetrofillController.TEST_RUN).toBoolean()
                )

            return FlowExecutionStatus(RESULT_VALUE_MAP[executionTuple] ?: PASS_FULL)
        }
    }

    companion object {
        // Dangling rows | migration condition (three) | Test
        val PASS_FULL = "PASS_FULL"
        val PASS_PARTIAL = "PASS_PARTIAL"
        val PASS_PARTIAL_WITHOUT_ATTR = "PASS_PARTIAL_WITHOUT_ATTR"
        val DETECT_FULL = "DETECT_FULL"
        val DETECT_PARTIAL = "DETECT_PARTIAL"
        val DETECT_PARTIAL_WITHOUT_ATTR = "DETECT_PARTIAL_WITHOUT_ATTR"
        val PASS_FULL_TEST = "PASS_FULL_TEST"
        val PASS_PARTIAL_TEST = "PASS_PARTIAL_TEST"
        val PASS_PARTIAL_WITHOUT_ATTR_TEST = "PASS_PARTIAL_WITHOUT_ATTR_TEST"
        val DETECT_FULL_TEST = "DETECT_FULL_TEST"
        val DETECT_PARTIAL_TEST = "DETECT_PARTIAL_TEST"
        val DETECT_PARTIAL_WITHOUT_ATTR_TEST = "DETECT_PARTIAL_WITHOUT_ATTR_TEST"

        val RESULT_VALUE_MAP: Map<Triple<Boolean, Pair<Boolean, Boolean>, Boolean>, String> = mapOf(
            Triple(true, Pair(false, false), false) to PASS_FULL,
            Triple(true, Pair(true, false), false) to PASS_PARTIAL,
            Triple(true, Pair(true, true), false) to PASS_PARTIAL_WITHOUT_ATTR,
            Triple(false, Pair(false, false), false) to DETECT_FULL,
            Triple(false, Pair(true, false), false) to DETECT_PARTIAL,
            Triple(false, Pair(true, true), false) to DETECT_PARTIAL_WITHOUT_ATTR,
            Triple(true, Pair(false, false), true) to PASS_FULL_TEST,
            Triple(true, Pair(true, false), true) to PASS_PARTIAL_TEST,
            Triple(true, Pair(true, true), true) to PASS_PARTIAL_WITHOUT_ATTR_TEST,
            Triple(false, Pair(false, false), true) to DETECT_FULL_TEST,
            Triple(false, Pair(true, false), true) to DETECT_PARTIAL_TEST,
            Triple(false, Pair(true, true), true) to DETECT_PARTIAL_WITHOUT_ATTR_TEST
        )
    }
}
