package com.labelbox.adv.retrofill.dao

import com.labelbox.adv.retrofill.clients.LaunchDarklyClient
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.ORG_IDS_PARAM
import com.labelbox.adv.retrofill.rest.RetrofillController.Companion.SKIP_LD_SETTING
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class LDJobExecutionListener : JobExecutionListener {

    @Autowired
    lateinit var ldClient: LaunchDarklyClient

    override fun beforeJob(jobExecution: JobExecution) {
        if (!jobExecution.jobParameters.getString(SKIP_LD_SETTING).toBoolean()) {
            val orgIds = jobExecution.jobParameters.getString(ORG_IDS_PARAM)?.split(",")
            if (orgIds != null) {
                val resp = ldClient.patchToDualWrite(orgIds)
                if (resp.code != 200) {
                    throw RuntimeException("Failed to patch $orgIds to DarklyLaunch as prep migration stage")
                }
            }
        }
    }

    override fun afterJob(jobExecution: JobExecution) {
        if (!jobExecution.jobParameters.getString(SKIP_LD_SETTING).toBoolean()) {
            val orgIds = jobExecution.jobParameters.getString(ORG_IDS_PARAM)?.split(",")
            if (orgIds != null) {
                val resp = ldClient.patch(orgIds)
                if (resp.code != 200) {
                    throw RuntimeException("Failed to patch $orgIds to DarklyLaunch as migrated stage")
                }
            }
        }
    }
}
