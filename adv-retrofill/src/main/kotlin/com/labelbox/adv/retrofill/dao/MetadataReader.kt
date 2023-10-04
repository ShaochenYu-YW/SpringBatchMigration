package com.labelbox.adv.retrofill.dao

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.cloud.Timestamp
import com.google.cloud.spanner.Mutation
import com.labelbox.adv.retrofill.domain.ApiUser
import com.labelbox.adv.retrofill.domain.MetadataType
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
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.query
import org.springframework.stereotype.Component
import java.sql.ResultSet
import javax.sql.DataSource

data class EnumOption(
    val drid: String,
    var value: MutableList<String>,
    var createAt: java.sql.Timestamp,
    var updatedAt: java.sql.Timestamp
)

@Component
class MetadataReader {

    @Autowired
    @Qualifier("mysqlDataSource")
    lateinit var datasource: DataSource

    @Autowired
    lateinit var jdbcTemplate: JdbcTempl

    fun synchronizedMetadataPagingReader(
        pattern: DataRowReaderQueryUtil.QueryPattern,
        ids: String,
        metadataSchema: Map<String, Map<String, String>>,
        dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<List<Mutation>> {
        val reader = JdbcPagingItemReader<List<Mutation>>()
        val idsList = ids.split(",")
        val bindingArgs = idsList.joinToString { "?" }
        val paramValue = idsList.mapIndexed { index, value -> (index + 1).toString() to value }.toMap()
        reader.setDataSource(datasource)
        reader.pageSize = FETCH_SIZE
        reader.setFetchSize(FETCH_SIZE)
        reader.setRowMapper { rs, _ -> getMutationListForMetadata(rs, metadataSchema) }
        val queryProvider = SqlPagingQueryProviderFactoryBean()
        queryProvider.setDataSource(datasource)
        queryProvider.setSelectClause(
            """
            DRCM.updatedAt,
            DRCM.createdAt,
            DRCM.dataRowId,
            JSON_REMOVE(DRCM.jsonAttributes,'$.ckrzang79000008l6hb5s6za1', '$.ckpyije740000yxdk81pbgjdc', '$.ckwv9uzy90000wayj7yb6h31u', '$.ckrzao09x000108l67vrcdnh3', '$.ckrzao5r3000208l6e4wr0zb6', '$.cko8sbczn0002h2dkdaxb5kal') as jsonAttributes
            """
        )

        val checkpointClause = if (dataRowCheckPoint.isNotEmpty()) { " AND DR.id >= '$dataRowCheckPoint'" } else { "" }
        when (pattern) {
            DataRowReaderQueryUtil.QueryPattern.DANGLING_ORG_ID -> {
                queryProvider.setFromClause("DataRow DR INNER JOIN Dataset DS on DR.datasetId = DS.id INNER JOIN DataRowCustomMetadata DRCM on DR.id = DRCM.dataRowId")
                queryProvider.setWhereClause("DS.organizationId IN ($bindingArgs) AND DS.deleted = 0 AND DR.deletedAt is NULL$checkpointClause")
            }

            DataRowReaderQueryUtil.QueryPattern.ORG_ID -> {
                queryProvider.setFromClause("DataRow DR INNER JOIN DataRowCustomMetadata DRCM on DR.id = DRCM.dataRowId")
                queryProvider.setWhereClause("DR.organizationId IN ($bindingArgs) AND DR.deletedAt is NULL$checkpointClause")
            }

            DataRowReaderQueryUtil.QueryPattern.DATASET_ID -> {
                queryProvider.setFromClause("DataRow DR INNER JOIN DataRowCustomMetadata DRCM on DR.id = DRCM.dataRowId")
                queryProvider.setWhereClause("DR.datasetId IN ($bindingArgs) AND DR.deletedAt is NULL$checkpointClause")
            }
            DataRowReaderQueryUtil.QueryPattern.DATAROW_ID -> {
                queryProvider.setFromClause("DataRow DR INNER JOIN DataRowCustomMetadata DRCM on DR.id = DRCM.dataRowId")
                queryProvider.setWhereClause("DR.id IN ($bindingArgs) AND DR.deletedAt is NULL")
            }
        }

        queryProvider.setSortKeys(mapOf("DRCM.dataRowId" to Order.ASCENDING))
        reader.setQueryProvider(queryProvider.getObject())
        reader.setParameterValues(paramValue)
        reader.afterPropertiesSet()

        val synchronizedReader = SynchronizedItemStreamReader<List<Mutation>>()
        synchronizedReader.setDelegate(reader)
        return synchronizedReader
    }

    @Bean
    @StepScope
    fun synchronizedMetedataReaderWithDanglingRows(
        @Value("#{jobParameters['orgIds']}") orgIds: String,
        @Value("#{jobParameters['dataRowCheckPoint']}") dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<List<Mutation>> {
        val ids = orgIds.split(",")
        return synchronizedMetadataPagingReader(DataRowReaderQueryUtil.QueryPattern.DANGLING_ORG_ID, orgIds, getMetadataSchema(ids), dataRowCheckPoint)
    }

    @Bean
    @StepScope
    fun synchronizedMetadataReader(
        @Value("#{jobParameters['orgIds']}") orgIds: String,
        @Value("#{jobParameters['dataRowCheckPoint']}") dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<List<Mutation>> {
        val ids = orgIds.split(",")
        return synchronizedMetadataPagingReader(DataRowReaderQueryUtil.QueryPattern.ORG_ID, orgIds, getMetadataSchema(ids), dataRowCheckPoint)
    }

    @Bean
    @StepScope
    fun synchronizedMetadataReaderWithDatasetIds(
        @Value("#{jobParameters['datasetIds']}") datasetIds: String,
        @Value("#{jobParameters['dataRowCheckPoint']}") dataRowCheckPoint: String
    ): SynchronizedItemStreamReader<List<Mutation>> {
        val ids = getOrgId(datasetIds)
        return synchronizedMetadataPagingReader(DataRowReaderQueryUtil.QueryPattern.DATASET_ID, datasetIds, getMetadataSchema(ids), dataRowCheckPoint)
    }

    @Bean
    @StepScope
    fun synchronizedMetadataReaderWithDataRowIds(
        @Value("#{jobParameters['dataRowIds']}") dataRowIds: String
    ): SynchronizedItemStreamReader<List<Mutation>> {
        val ids = getOrgIdFromDataRow(dataRowIds)
        return synchronizedMetadataPagingReader(DataRowReaderQueryUtil.QueryPattern.DATAROW_ID, dataRowIds, getMetadataSchema(ids), "")
    }

    fun getMetadataSchema(
        orgIdsList: List<String>
    ): Map<String, Map<String, String>> {
        val bindingVars = orgIdsList.joinToString(",") { "?" }
        logger.info("getMetadata is on $orgIdsList")
        val query =
            """
               SELECT
                    FS.id,
                    JSON_EXTRACT(FS.definition, '$.title') as title,
                    FS.kind,
                    SNP.schemaId AS parentSchemaId
                FROM
                    FeatureSchema FS
                        INNER JOIN SchemaNode SN ON (SN.schemaId = FS.id)
                        LEFT JOIN SchemaNode SNP ON (SNP.id = SN.parentId)
                WHERE
                    SN.ontologyId IN (SELECT datarowMetadataOntologyId FROM Organization WHERE id IN ($bindingVars))
                AND
                    SN.organizationId IN ($bindingVars)
                AND
                    SN.deleted = 0
            """
        val orgIdArgs = orgIdsList.toTypedArray()
        return jdbcTemplate.query(query, *orgIdArgs, *orgIdArgs) { rs, _ ->
            val id = rs.getString("id")
            val title = rs.getString("title").replace("\"", "")
            val kind = rs.getString("kind")
            val parentSchemaId = rs.getString("parentSchemaId")
            id to mapOf("title" to title, "kind" to kind, "parentSchemaId" to parentSchemaId)
        }.toMap()
    }

    @Bean
    @StepScope
    fun getOrgId(
        datasetIds: String
    ): List<String> {
        val datasetIdsList = datasetIds.split(",")
        val query =
            """
                SELECT organizationId
                from Dataset DS
                where DS.id=?
                and DS.deleted = 0 
            """
        // This is fine as long as first dataset id is not deleted.
        val datasetIdArgs = listOfNotNull(datasetIdsList[0]).toTypedArray()
        return jdbcTemplate.query(query, *datasetIdArgs) { rs, _ ->
            rs.getString("organizationId")
        }.toList()
    }

    @Bean
    @StepScope
    fun getOrgIdFromDataRow(
        dataRowIds: String
    ): List<String> {
        val dataRowIdsList = dataRowIds.split(",")
        val query =
            """
                SELECT organizationId
                from DataRow
                where id=?
                and deletedAt is NULL
            """
        // This is fine as long as first data row is valid.
        val dataRowIdArgs = listOfNotNull(dataRowIdsList[0]).toTypedArray()
        return jdbcTemplate.query(query, *dataRowIdArgs) { rs, _ ->
            rs.getString("organizationId")
        }.toList()
    }

    fun getMutationListForMetadata(rs: ResultSet, metadataSchema: Map<String, Map<String, String>>): List<Mutation> {
        val drid = rs.getString("dataRowId").reversed()
        val mutationList = mutableListOf<Mutation>()
        val attrs: MutableMap<String, Any> = mapper.readValue<Map<String, Any>>(rs.getString("jsonAttributes")).toMutableMap()
        val enumOptions = mutableMapOf<String, EnumOption>()
        for ((schemaId, json) in attrs) {
            val (type, value) = when (schemaId) {
                in reservedNonEmbeddingSchemas -> {
                    val type = reservedNonEmbeddingSchemas[schemaId]?.get("kind")?.let { MetadataType.fromString(it) }
                    val parentId = if (type == MetadataType.ENUM) reservedNonEmbeddingSchemas[schemaId]?.get("parentSchemaId") else null
                    if (parentId != null) {
                        enumOptions.getOrPut(parentId) {
                            EnumOption(drid, mutableListOf(), rs.getTimestamp("createdAt"), rs.getTimestamp("updatedAt"))
                        }.value.add(schemaId)
                        continue
                    }
                    val value = reservedNonEmbeddingSchemas[schemaId]?.get("value") ?: json
                    Pair(type, value)
                }
                in metadataSchema -> {
                    val type = metadataSchema[schemaId]?.get("kind")?.let { MetadataType.fromString(it) }
                    val parentId = if (type == MetadataType.ENUM) metadataSchema[schemaId]?.get("parentSchemaId") else null
                    if (parentId != null) {
                        enumOptions.getOrPut(parentId) {
                            EnumOption(drid, mutableListOf(), rs.getTimestamp("createdAt"), rs.getTimestamp("updatedAt"))
                        }.value.add(schemaId)
                        continue
                    }
                    val value =
                        when (type) {
                            MetadataType.NUMBER -> json.toString()
                            else -> json
                        }
                    Pair(type, value)
                }
                else -> {
                    continue
                }
            }

            val builder = Mutation.newInsertOrUpdateBuilder("DataRowMetadata")
                .set("dataRowId").to(drid)
                .set("schemaId").to(schemaId)
                .set("type").to(type?.ordinal?.toLong() ?: MetadataType.STRING.ordinal.toLong())
                .set("value").to(com.google.cloud.spanner.Value.json(mapper.writeValueAsString(value)))
                .set("shardId").to(ShardRange.getShardKey(drid).toLong())
                .set("createdAt").to(Timestamp.of(rs.getTimestamp("createdAt")))
                .set("updatedAt").to(Timestamp.of(rs.getTimestamp("updatedAt")))
                .set("createdBy").to(ApiUser.userId)
                .set("updatedBy").to(ApiUser.userId)

            mutationList.add(builder.build())
        }

        for ((parentId, enumOption) in enumOptions) {
            val builder = Mutation.newInsertOrUpdateBuilder("DataRowMetadata")
                .set("dataRowId").to(enumOption.drid)
                .set("schemaId").to(parentId)
                .set("type").to(MetadataType.ENUM.ordinal.toLong())
                .set("value").to(com.google.cloud.spanner.Value.json(mapper.writeValueAsString(enumOption.value)))
                .set("shardId").to(ShardRange.getShardKey(enumOption.drid).toLong())
                .set("createdAt").to(Timestamp.of(enumOption.createAt))
                .set("updatedAt").to(Timestamp.of(enumOption.updatedAt))
                .set("createdBy").to(ApiUser.userId)
                .set("updatedBy").to(ApiUser.userId)

            mutationList.add(builder.build())
        }

        return mutationList
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
        private val FETCH_SIZE = 300_000

        private val reservedEmbeddingSchemaIds = setOf(
            "ckpyije740000yxdk81pbgjdc", // RESERVED_METADATA_SCHEMA_EMBEDDING_CUID
            "ckrzang79000008l6hb5s6za1", // RESERVED_METADATA_SCHEMA_PRECOMPUTED_IMAGE_EMBEDDING_CUID
            "ckwv9uzy90000wayj7yb6h31u", // RESERVED_METADATA_SCHEMA_PRECOMPUTED_GEOSPATIAL_IMAGE_EMBEDDING_CUID
            "ckrzao09x000108l67vrcdnh3", // RESERVED_METADATA_SCHEMA_PRECOMPUTED_TEXT_EMBEDDING_CUID
            "ckrzao5r3000208l6e4wr0zb6" // RESERVED_METADATA_SCHEMA_PRECOMPUTED_VIDEO_EMBEDDING_CUID
        )

        private val reservedNonEmbeddingSchemas = mapOf(
            "cko8sc2yr0004h2dk69aj5x63" to // RESERVED_METADATA_SCHEMA_SPLIT_VALID_CUID
                mapOf("kind" to "CustomMetadataEnumOption", "value" to "valid", "parentSchemaId" to "cko8sbczn0002h2dkdaxb5kal"),
            "cko8s9r5v0001h2dk9elqdidh" to // RESERVED_METADATA_SCHEMA_TAG_CUID
                mapOf("kind" to "CustomMetadataString"),
            "cko8sbscr0003h2dk04w86hof" to // RESERVED_METADATA_SCHEMA_SPLIT_TRAIN_CUID
                mapOf("kind" to "CustomMetadataEnumOption", "value" to "train", "parentSchemaId" to "cko8sbczn0002h2dkdaxb5kal"),
            "cko8scbz70005h2dkastwhgqt" to // RESERVED_METADATA_SCHEMA_SPLIT_TEST_CUID
                mapOf("kind" to "CustomMetadataEnumOption", "value" to "test", "parentSchemaId" to "cko8sbczn0002h2dkdaxb5kal"),
            "cko8sdzv70006h2dk8jg64zvb" to // RESERVED_METADATA_SCHEMA_CAPTURE_DATE_TIME_TEST_CUID
                mapOf("kind" to "CustomMetadataDateTime")
        )

        private val mapper = jacksonObjectMapper()
    }
}
