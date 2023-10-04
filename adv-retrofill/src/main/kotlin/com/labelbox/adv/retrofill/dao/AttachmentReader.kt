package com.labelbox.adv.retrofill.dao

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.labelbox.adv.retrofill.domain.ApiUser
import com.labelbox.adv.retrofill.domain.AttachmentType
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
class AttachmentReader {

    @Autowired
    @Qualifier("mysqlDataSource")
    lateinit var datasource: DataSource

    fun synchronizedAttachmentPagingReader(
        pattern: DataRowReaderQueryUtil.QueryPattern,
        ids: String,
        dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<Mutation> {
        val idsList = ids.split(",")
        val bindingArgs = idsList.joinToString { "?" }
        val paramValue = idsList.mapIndexed { index, value -> (index + 1).toString() to value }.toMap()
        val reader = JdbcPagingItemReader<Mutation>()
        reader.setDataSource(datasource)
        reader.pageSize = FETCH_SIZE
        reader.setFetchSize(FETCH_SIZE)
        reader.setRowMapper { rs, _ -> getMutationForAttachment(rs) }

        val queryProvider = SqlPagingQueryProviderFactoryBean()
        queryProvider.setDataSource(datasource)
        queryProvider.setSelectClause(
            """
            DR.id,
            AM.id as attachmentId,
            AM.createdAt as createdAt,
            AM.updatedAt as updatedAt,
            AM.metaType as type,
            AM.metaValue as value,
            AM.name as attachmentName
            """
        )

        val checkpointClause = if (dataRowCheckPoint.isNotEmpty()) { " AND DR.id >= '$dataRowCheckPoint'" } else { "" }
        when (pattern) {
            DataRowReaderQueryUtil.QueryPattern.DANGLING_ORG_ID -> {
                queryProvider.setFromClause("DataRow DR INNER JOIN Dataset DS on DR.datasetId = DS.id INNER JOIN _DataRowMetadata DRM on DR.id = DRM.B INNER JOIN AssetMetadata AM on DRM.A = AM.id")
                queryProvider.setWhereClause("DS.organizationId IN ($bindingArgs) AND DS.deleted = 0 AND DR.deletedAt is NULL$checkpointClause")
            }

            DataRowReaderQueryUtil.QueryPattern.ORG_ID -> {
                queryProvider.setFromClause("DataRow DR INNER JOIN _DataRowMetadata DRM on DR.id = DRM.B INNER JOIN AssetMetadata AM on DRM.A = AM.id")
                queryProvider.setWhereClause("DR.organizationId IN ($bindingArgs) AND DR.deletedAt is NULL$checkpointClause")
            }

            DataRowReaderQueryUtil.QueryPattern.DATASET_ID -> {
                queryProvider.setFromClause("DataRow DR INNER JOIN _DataRowMetadata DRM on DR.id = DRM.B INNER JOIN AssetMetadata AM on DRM.A = AM.id")
                queryProvider.setWhereClause("DR.datasetId IN ($bindingArgs) AND DR.deletedAt is NULL$checkpointClause")
            }

            DataRowReaderQueryUtil.QueryPattern.DATAROW_ID -> {
                queryProvider.setFromClause("DataRow DR INNER JOIN _DataRowMetadata DRM on DR.id = DRM.B INNER JOIN AssetMetadata AM on DRM.A = AM.id")
                queryProvider.setWhereClause("DR.id IN ($bindingArgs) AND DR.deletedAt is NULL")
            }
        }

        queryProvider.setSortKeys(mapOf("DR.id" to Order.ASCENDING))
        reader.setQueryProvider(queryProvider.getObject())
        reader.setParameterValues(paramValue)
        reader.afterPropertiesSet()

        val synchronizedReader = SynchronizedItemStreamReader<Mutation>()
        synchronizedReader.setDelegate(reader)
        return synchronizedReader
    }

    @Bean
    @StepScope
    fun synchronizedAttachmentReaderWithDanglingRows(
        @Value("#{jobParameters['orgIds']}") orgIds: String,
        @Value("#{jobParameters['dataRowCheckPoint']}") dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<Mutation> {
        return synchronizedAttachmentPagingReader(DataRowReaderQueryUtil.QueryPattern.DANGLING_ORG_ID, orgIds, dataRowCheckPoint)
    }

    @Bean
    @StepScope
    fun synchronizedAttachmentReader(
        @Value("#{jobParameters['orgIds']}") orgIds: String,
        @Value("#{jobParameters['dataRowCheckPoint']}") dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<Mutation> {
        return synchronizedAttachmentPagingReader(DataRowReaderQueryUtil.QueryPattern.ORG_ID, orgIds, dataRowCheckPoint)
    }

    @Bean
    @StepScope
    fun synchronizedAttachmentReaderWithDatasetIds(
        @Value("#{jobParameters['datasetIds']}") datasetIds: String,
        @Value("#{jobParameters['dataRowCheckPoint']}") dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<Mutation> {
        return synchronizedAttachmentPagingReader(DataRowReaderQueryUtil.QueryPattern.DATASET_ID, datasetIds, dataRowCheckPoint)
    }

    @Bean
    @StepScope
    fun synchronizedAttachmentReaderWithDataRowIds(
        @Value("#{jobParameters['dataRowIds']}") dataRowIds: String
    ): SynchronizedItemStreamReader<Mutation> {
        return synchronizedAttachmentPagingReader(DataRowReaderQueryUtil.QueryPattern.DATAROW_ID, dataRowIds, "")
    }

    fun getMutationForAttachment(rs: ResultSet): Mutation? {
        val attType = AttachmentType.fromString(rs.getString("type"))
        if (attType != null) {
            return Mutation.newInsertOrUpdateBuilder("Attachment")
                .set("dataRowId")
                .to(rs.getString("id").reversed())
                .set("attachmentId")
                .to(rs.getString("attachmentId"))
                .set("createdAt")
                .to(Timestamp.of(rs.getTimestamp("createdAt")))
                .set("updatedAt")
                .to(Timestamp.of(rs.getTimestamp("updatedAt")))
                .set("type").to(attType.ordinal.toLong())
                .set("value")
                .to(rs.getString("value"))
                .set("createdBy")
                .to(ApiUser.userId)
                .set("updatedBy")
                .to(ApiUser.userId)
                .set("name")
                .to(rs.getString("attachmentName"))
                .build()
        } else {
            return null
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
        private val FETCH_SIZE = 500_000
    }
}
