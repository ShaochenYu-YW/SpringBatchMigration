package com.labelbox.adv.retrofill.dao

import com.slack.api.Slack
import com.slack.api.webhook.Payload
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import java.time.Duration

@Component
@ConfigurationProperties(prefix = "spring.slack")
class SlackConfiguration {
    lateinit var url: String
}

@Component
class SlackJobExecutionListener(config: SlackConfiguration) : JobExecutionListener {
    private val url = config.url
    private val slack = Slack.getInstance()

    override fun beforeJob(jobExecution: JobExecution) {
        val payload = Payload.builder()
            .text("Starting ADV migration Job id: ${jobExecution.jobId} with job Parameter: ${jobExecution.jobParameters}").build()
        slack.send(url, payload)
    }

    override fun afterJob(jobExecution: JobExecution) {
        var text = when (jobExecution.status) {
            BatchStatus.COMPLETED -> "Successfully completed Job id: ${jobExecution.jobId} with job Parameter: ${jobExecution.jobParameters}"
            BatchStatus.FAILED -> "Failed Job id: ${jobExecution.jobId} with failure ${jobExecution.allFailureExceptions}"
            else ->
                if (jobExecution.exitStatus.exitCode == "NOOP") {
                    "ADV Migration job id: ${jobExecution.jobId} get skipped with invalid ids or dataset ids not being in the same org"
                } else {
                    "Completed ADV migration Job id: ${jobExecution.jobId} with status: ${jobExecution.status}"
                }
        }
        val incrementalJobInfo =
            if (jobExecution.executionContext.containsKey("incremental_jobs")) {
                "\nTriggers incremental jobs: ${jobExecution.executionContext.get("incremental_jobs")}"
            } else {
                "\nNo incremental job get triggered"
            }

        text = text.plus(incrementalJobInfo)

        val skippedDataRows =
            if (jobExecution.executionContext.containsKey("skipped_drs")) {
                val drsList = jobExecution.executionContext.get("skipped_drs") as MutableList<String>
                val concatenatedString = drsList.joinToString(separator = ", ", prefix = "[", postfix = "]")
                "\nSkipped data rows: $concatenatedString"
            } else {
                "\nNo data row got skipped."
            }

        text = text.plus(skippedDataRows)

        val totalTime = Duration.between(jobExecution.createTime, jobExecution.endTime)
        val minutes = totalTime.toMinutes()

        text = text.plus("\nTotal execution time as $minutes minutes.")
        if (jobExecution.exitStatus.exitCode != "NOOP") {
            text = text.plus("\nExecution Summary:\n")
            text = text.plus(getJobSummary(jobExecution))
        }
        val payload = Payload.builder()
            .text(text).build()
        slack.send(url, payload)
        // Ignore IOException for send method
    }

    private fun getJobSummary(jobExecution: JobExecution): String {
        val map: MutableMap<String, String> = LinkedHashMap()
        for (stepExecution in jobExecution.stepExecutions) {
            map[stepExecution.stepName] = stepExecution.toString()
        }
        return createSlackPayload(map)
    }

    fun createSlackPayload(map: MutableMap<String, String>): String {
        val stringBuilder = StringBuilder()
        stringBuilder.append("```")
        for (entry in map.entries) {
            stringBuilder.append(entry.key)
            stringBuilder.append(": ")
            stringBuilder.append(entry.value.replace(",", ",\n\t"))
            stringBuilder.append("\n")
        }
        stringBuilder.append("```")
        return stringBuilder.toString()
    }
}
