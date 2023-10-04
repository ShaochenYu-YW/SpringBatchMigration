package com.labelbox.adv.retrofill.domain

enum class RowDataType {
    URI,
    EMBEDDED;

    companion object {
        fun detect(rowData: String): RowDataType {
            return regex.find(rowData)?.let {
                URI
            } ?: EMBEDDED
        }

        private val regex = Regex("^(https?|gs)://", RegexOption.IGNORE_CASE)
    }
}
