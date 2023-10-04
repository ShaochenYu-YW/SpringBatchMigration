package com.labelbox.adv.retrofill.domain

data class MigrationOrgsRequest(
    val orgIds: List<String>,
    val splitExecution: Boolean = true,
    val skipLdSetting: Boolean = true,
    val skipDataRow: Boolean = false,
    val skipAttachment: Boolean = false,
    val isTestRun: Boolean = false,
    val dataRowCheckpoint: String = "",
    val enableIncrementalUpdate: Boolean = false,
    val incUpdateWindow: Long = -1
)

data class MigrationDataSetRequest(
    val datasetIds: List<String>,
    val skipDataRow: Boolean = false,
    val skipAttachment: Boolean = false,
    val dataRowCheckpoint: String = ""
)

data class MigrationDataRowRequest(
    val dataRowIds: List<String>,
    val isTestRun: Boolean = false
)
