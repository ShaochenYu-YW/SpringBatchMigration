package com.labelbox.adv.retrofill.dao

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.labelbox.adv.retrofill.domain.ApiUser
import com.labelbox.adv.retrofill.domain.ShardRange
import org.slf4j.LoggerFactory
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.support.SynchronizedItemStreamReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component
import java.sql.ResultSet
import javax.sql.DataSource

@Component
class DatasetReader {

    @Autowired
    @Qualifier("mysqlDataSource")
    lateinit var datasource: DataSource

    fun getSynchronizedStoreDatasetReader(
        ids: String?,
        isOrgLevel: Boolean
    ): SynchronizedItemStreamReader<Mutation> {
        val idsList = ids?.split(",")
        val bindingArgs = idsList?.joinToString { "?" }
        val reader = JdbcCursorItemReader<Mutation>()
        reader.dataSource = datasource
        reader.sql =
            if (isOrgLevel) {
                "SELECT id, organizationId, name, createdAt, updatedAt, name, description FROM Dataset WHERE organizationId IN ($bindingArgs) AND deleted = 0"
            } else {
                "SELECT id, organizationId, name, createdAt, updatedAt, name, description FROM Dataset WHERE id IN ($bindingArgs) AND deleted = 0"
            }
        reader.setRowMapper { rs, _ ->
            getMutationForDataset(rs)
        }
        reader.setVerifyCursorPosition(false)
        reader.setPreparedStatementSetter { ps ->
            idsList?.forEachIndexed { index, orgId ->
                ps.setString(index + 1, orgId)
            }
        }

        reader.setFetchSize(FETCH_SIZE)

        val synchronizedReader = SynchronizedItemStreamReader<Mutation>()
        synchronizedReader.setDelegate(reader)
        return synchronizedReader
    }

    @Bean
    @StepScope
    fun synchronizedStoreDatasetReader(
        @Value("#{jobParameters['orgIds']}") orgIds: String?
    ): SynchronizedItemStreamReader<Mutation> {
        return getSynchronizedStoreDatasetReader(orgIds, true)
    }

    @Bean
    @StepScope
    fun synchronizedStoreDatasetReaderWithDatasetIds(
        @Value("#{jobParameters['datasetIds']}") datasetIds: String?
    ): SynchronizedItemStreamReader<Mutation> {
        return getSynchronizedStoreDatasetReader(datasetIds, false)
    }

    fun getMutationForDataset(rs: ResultSet): Mutation {
        val dsid = rs.getString("id")
        return Mutation.newInsertOrUpdateBuilder("Dataset")
            .set("datasetId")
            .to(rs.getString("id"))
            .set("organizationId")
            .to(rs.getString("organizationId"))
            .set("shardId")
            .to(ShardRange.getShardKey(dsid).toLong())
            .set("createdAt")
            .to(Timestamp.of(rs.getTimestamp("createdAt")))
            .set("updatedAt")
            .to(Timestamp.of(rs.getTimestamp("updatedAt")))
            .set("createdBy")
            .to(ApiUser.userId)
            .set("updatedBy")
            .to(ApiUser.userId)
            .set("name")
            .to(rs.getString("name"))
            .set("description")
            .to(rs.getString("description"))
            .build()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
        private val FETCH_SIZE = 1000
    }
}
