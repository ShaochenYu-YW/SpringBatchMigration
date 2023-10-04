package com.labelbox.adv.retrofill.dao

import com.google.cloud.spanner.Mutation
import org.springframework.batch.core.SkipListener
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.item.ExecutionContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
@JobScope
class SkipExecutionListener(
    @Value("#{jobExecution.executionContext}")
    var executionContext: ExecutionContext
) : SkipListener<Mutation, Mutation> {

    override fun onSkipInWrite(item: Mutation, t: Throwable) {
        if (executionContext.containsKey(SKIPPED_DRS)) {
            val drList = executionContext.get(SKIPPED_DRS) as MutableList<String>
            item.asMap()["dataRowId"]?.let { drList.add(it.asString.reversed()) }
        } else {
            executionContext.put(SKIPPED_DRS, mutableListOf(item.asMap()["dataRowId"]))
        }
    }

    companion object {
        private const val SKIPPED_DRS = "skipped_drs"
    }
}
