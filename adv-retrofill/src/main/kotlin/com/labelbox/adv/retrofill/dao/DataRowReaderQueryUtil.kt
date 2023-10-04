package com.labelbox.adv.retrofill.dao

class DataRowReaderQueryUtil {

    enum class QueryPattern {
        DANGLING_ORG_ID,
        ORG_ID,
        DATASET_ID,
        DATAROW_ID
    }

    companion object {
        fun getDatasetBasedWhereClause(bindingArgsCnt: Int): String {
            val bindingArgs = List(bindingArgsCnt) { "?" }.joinToString(", ")
            return "INNER JOIN Dataset DS on DR.datasetId = DS.id WHERE DS.organizationId IN ($bindingArgs) AND DS.deleted = 0"
        }

        fun getDataRowBasedWhereClause(bindingArgsCnt: Int): String {
            val bindingArgs = List(bindingArgsCnt) { "?" }.joinToString(", ")
            return "WHERE DR.organizationId IN ($bindingArgs) AND DR.deletedAt is NULL"
        }

        fun getDatasetIdsWhereClause(bindingArgsCnt: Int): String {
            val bindingArgs = List(bindingArgsCnt) { "?" }.joinToString(", ")
            return "WHERE DR.datasetId IN ($bindingArgs) AND DR.deletedAt is NULL"
        }

        fun getDataRowIdsWhereClause(bindingArgsCnt: Int): String {
            val bindingArgs = List(bindingArgsCnt) { "?" }.joinToString(", ")
            return "WHERE DR.id IN ($bindingArgs) AND DR.deletedAt is NULL"
        }
    }
}
