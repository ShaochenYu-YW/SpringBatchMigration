package com.labelbox.adv.retrofill.dao

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.labelbox.adv.retrofill.domain.ApiUser
import com.labelbox.adv.retrofill.domain.RowDataType
import com.labelbox.adv.retrofill.domain.ShardRange
import org.slf4j.LoggerFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.database.JdbcPagingItemReader
import org.springframework.batch.item.database.Order
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean
import org.springframework.batch.item.support.SynchronizedItemStreamReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import java.sql.ResultSet
import javax.sql.DataSource

@Component
class DataRowReader {

    @Autowired
    @Qualifier("mysqlDataSource")
    lateinit var datasource: DataSource

    fun getSynchronizedDataRowPagingReader(
        pattern: DataRowReaderQueryUtil.QueryPattern,
        ids: String,
        dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<Mutation> {
        val reader = JdbcPagingItemReader<Mutation>()
        val idsList = ids.split(",")
        val bindingArgs = idsList.joinToString { "?" }
        reader.setDataSource(datasource)
        reader.pageSize = DATA_ROW_FETCH_SIZE
        reader.setFetchSize(DATA_ROW_FETCH_SIZE)
        reader.setRowMapper { rs, _ -> getMutationForDataRow(rs) }
        val queryProvider = SqlPagingQueryProviderFactoryBean()
        queryProvider.setDataSource(datasource)
        queryProvider.setSelectClause(
            """
            DR.id as id,
            DR.organizationId as organizationId,
            DR.datasetId as datasetId,
            DR.createdAt as createdAt,
            DR.updatedAt as updatedAt,
            DR.rowData as rowData,
            DR.globalKey as globalKey,
            DR.externalId as externalId,
            MAG.attributes as mediaAttributes
            """
        )

        val checkpointClause = if (dataRowCheckPoint.isNotEmpty()) { " AND DR.id >= '$dataRowCheckPoint'" } else { "" }
        when (pattern) {
            DataRowReaderQueryUtil.QueryPattern.DANGLING_ORG_ID -> {
                queryProvider.setFromClause("DataRow DR INNER JOIN Dataset DS on DR.datasetId = DS.id LEFT JOIN MediaAttributesGroup MAG on MAG.dataRowId = DR.id")
                queryProvider.setWhereClause(
                    "DS.organizationId IN ($bindingArgs) AND DS.deleted = 0 AND DR.deletedAt is NULL AND" +
                        "(externalId IS null OR DR.externalId != 'ADV_data_row')$checkpointClause"
                )
            }

            DataRowReaderQueryUtil.QueryPattern.ORG_ID -> {
                queryProvider.setFromClause("DataRow DR LEFT JOIN MediaAttributesGroup MAG on MAG.dataRowId = DR.id")
                queryProvider.setWhereClause(
                    "DR.organizationId IN ($bindingArgs) AND DR.deletedAt is NULL AND " +
                        "(externalId IS null OR DR.externalId != 'ADV_data_row')$checkpointClause"
                )
            }

            DataRowReaderQueryUtil.QueryPattern.DATASET_ID -> {
                queryProvider.setFromClause("DataRow DR LEFT JOIN MediaAttributesGroup MAG on MAG.dataRowId = DR.id")
                queryProvider.setWhereClause(
                    "DR.datasetId IN ($bindingArgs) AND DR.deletedAt is NULL AND " +
                        "(externalId IS null OR DR.externalId != 'ADV_data_row')$checkpointClause"
                )
            }

            DataRowReaderQueryUtil.QueryPattern.DATAROW_ID -> {
                queryProvider.setFromClause("DataRow DR LEFT JOIN MediaAttributesGroup MAG on MAG.dataRowId = DR.id")
                queryProvider.setWhereClause("DR.id IN ($bindingArgs) AND DR.deletedAt is NULL AND (externalId IS null OR DR.externalId != 'ADV_data_row')")
            }
        }

        queryProvider.setSortKeys(mapOf("DR.id" to Order.ASCENDING))
        reader.setQueryProvider(queryProvider.getObject())
        reader.setParameterValues(createParameterSourceFactory(ids))
        reader.afterPropertiesSet()

        val synchronizedReader = SynchronizedItemStreamReader<Mutation>()
        synchronizedReader.setDelegate(reader)
        return synchronizedReader
    }

    private fun createParameterSourceFactory(ids: String): Map<String, String> {
        return ids.split(",").mapIndexed { index, value -> (index + 1).toString() to value }.toMap()
    }

    @Bean
    @StepScope
    fun synchronizedDataRowReaderWithDanglingRows(
        @Value("#{jobParameters['orgIds']}") orgIds: String,
        @Value("#{jobParameters['dataRowCheckPoint']}") dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<Mutation> {
        return getSynchronizedDataRowPagingReader(DataRowReaderQueryUtil.QueryPattern.DANGLING_ORG_ID, orgIds, dataRowCheckPoint)
    }

    @Bean
    @StepScope
    fun synchronizedDataRowReader(
        @Value("#{jobParameters['orgIds']}") orgIds: String,
        @Value("#{jobParameters['dataRowCheckPoint']}") dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<Mutation> {
        return getSynchronizedDataRowPagingReader(DataRowReaderQueryUtil.QueryPattern.ORG_ID, orgIds, dataRowCheckPoint)
    }

    @Bean
    @StepScope
    fun synchronizedDataRowReaderWithDatasetIds(
        @Value("#{jobParameters['datasetIds']}") datasetIds: String,
        @Value("#{jobParameters['dataRowCheckPoint']}") dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<Mutation> {
        return getSynchronizedDataRowPagingReader(DataRowReaderQueryUtil.QueryPattern.DATASET_ID, datasetIds, dataRowCheckPoint)
    }

    @Bean
    @StepScope
    fun synchronizedDataRowReaderWithDataRowIds(
        @Value("#{jobParameters['dataRowIds']}") dataRowIds: String
    ): SynchronizedItemStreamReader<Mutation> {
        return getSynchronizedDataRowPagingReader(DataRowReaderQueryUtil.QueryPattern.DATAROW_ID, dataRowIds, "")
    }

    fun getMutationForDataRow(rs: ResultSet): Mutation {
        val drid = rs.getString("id").reversed()
        return Mutation.newInsertOrUpdateBuilder("DataRow")
            .set("dataRowId")
            .to(drid)
            .set("organizationId")
            .to(rs.getString("organizationId"))
            .set("datasetId")
            .to(rs.getString("datasetId"))
            .set("shardId")
            .to(ShardRange.getShardKey(drid).toLong())
            .set("createdAt")
            .to(Timestamp.of(rs.getTimestamp("createdAt")))
            .set("updatedAt")
            .to(Timestamp.of(rs.getTimestamp("updatedAt")))
            .set("rowData")
            .to(rs.getString("rowData"))
            .set("rowDataType")
            .to(RowDataType.detect(rs.getString("rowData")).ordinal.toLong())
            .set("globalKey")
            .to(rs.getString("globalKey"))
            .set("globalKeyHash")
            .to(IdGenerator.murmurHash32(rs.getString("globalKey"))?.toLong())
            .set("externalId")
            .to(rs.getString("externalId"))
            .set("createdBy")
            .to(ApiUser.userId)
            .set("updatedBy")
            .to(ApiUser.userId)
            .set("mediaAttributes")
            .to(
                rs.getString("mediaAttributes")?.let {
                    com.google.cloud.spanner.Value.json(it)
                } ?: com.google.cloud.spanner.Value.json(
                    "{}"
                )
            )
            .build()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
        private val DATA_ROW_FETCH_SIZE = 500_000
    }
}
