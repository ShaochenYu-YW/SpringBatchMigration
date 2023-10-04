package com.labelbox.adv.retrofill.dao

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.SearchRequest
import com.labelbox.adv.retrofill.batching.DataRowLevelMigrationBatchJobConfig
import com.labelbox.adv.retrofill.batching.DatasetLevelMigrationBatchJobConfig
import com.labelbox.adv.retrofill.rest.RetrofillController
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.DATAROW_IDS_PARAM
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.INC_UPDATE_MINS
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.ORG_IDS_PARAM
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.TEST_RUN
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.job.flow.FlowExecutionStatus
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.io.StringReader
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

@Component
class EsIncrementalUpdater : JobExecutionDecider {

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    @Qualifier("elasticsearchClient")
    lateinit var esClient: ElasticsearchClient

    @Autowired
    lateinit var datasetMigrationBatchJobConfig: DatasetLevelMigrationBatchJobConfig

    @Autowired
    lateinit var dataRowMigrationBatchJobConfig: DataRowLevelMigrationBatchJobConfig

    override fun decide(jobExecution: JobExecution, stepExecution: StepExecution?): FlowExecutionStatus {
        if (jobExecution.jobParameters.getString(RetrofillController.INC_UPDATE).toBoolean()) {
            val orgIds = jobExecution.jobParameters.getString(ORG_IDS_PARAM)
            if (orgIds == null) {
                throw RuntimeException("Missing datasetIds in the jobParameter!")
            } else {
                jobExecution.startTime?.let {
                    val incUpdateWindow = jobExecution.jobParameters.getLong(INC_UPDATE_MINS)
                    val windowSize =
                        if (incUpdateWindow?.toInt() == -1) {
                            ChronoUnit.MINUTES.between(it, LocalDateTime.now())
                        } else {
                            incUpdateWindow ?: 0L
                        }
                    val idsList = orgIds.split(",")
                    val incrementalJobs = mutableListOf<String>()
                    val dataRowsToUpdate = generateElasticsearchQuery(idsList, windowSize)
                    val isTestRun = jobExecution.jobParameters.getString(TEST_RUN)
                    for (i in 0 until dataRowsToUpdate.size step BATCH_SIZE) {
                        val dataRowIdBatch =
                            dataRowsToUpdate.subList(i, (i + BATCH_SIZE).coerceAtMost(dataRowsToUpdate.size))
                        val jobParameters = JobParametersBuilder()
                            .addLong("time", System.currentTimeMillis())
                            .addString(DATAROW_IDS_PARAM, dataRowIdBatch.joinToString(","))
                            .addString(TEST_RUN, isTestRun ?: "false")
                            .toJobParameters()
                        val je = dataRowMigrationBatchJobConfig.executeJob(jobParameters)
                        incrementalJobs.add(je.jobId.toString())
                    }

                    if (incrementalJobs.isNotEmpty()) {
                        jobExecution.executionContext.put("incremental_jobs", incrementalJobs.joinToString(","))
                    }
                }
            }
        }
        return FlowExecutionStatus(PASS_DETECTION)
    }

    fun generateElasticsearchQuery(organizationIds: List<String>, windowInMin: Long): List<String> {
        val organizationIdTerms = organizationIds.joinToString(",") { "\"$it\"" }

        val search = """
            {
              "query": {
                "bool": {
                  "must": [
                    {
                      "terms": {
                        "dataRow.organizationId": [$organizationIdTerms]
                      }
                    },
                    {
                      "range": {
                        "dataRow.lastActivityAt": {
                          "gte": "now-${windowInMin}m",
                          "lte": "now"
                        }
                      }
                    }
                  ]
                }
              }
            }
        """.trimIndent()

        val req = SearchRequest.Builder()
            .index(INDEX)
            .withJson(StringReader(search))
            .source { source -> source.fetch(false) }
            .size(MAX_SIZE)
            .build()

        return esClient.search(req, Any::class.java)
            .hits().hits().mapTo(ArrayList()) { it.id() }
    }

    companion object {
        val PASS_DETECTION = "PASS_DETECTION"
        val BATCH_SIZE = 30
        val INDEX = "catalog"
        val MAX_SIZE = 5_000
    }
}
