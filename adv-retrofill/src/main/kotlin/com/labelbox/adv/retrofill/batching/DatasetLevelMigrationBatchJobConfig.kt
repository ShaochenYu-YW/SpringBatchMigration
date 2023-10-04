package com.labelbox.adv.retrofill.batching

import com.google.cloud.spanner.Mutation
import com.labelbox.adv.retrofill.dao.DatasetOnSameOrgDetector
import com.labelbox.adv.retrofill.dao.DatasetOnSameOrgDetector.Companion.FULL_MIGRATION
import com.labelbox.adv.retrofill.dao.DatasetOnSameOrgDetector.Companion.PARTIAL_MIGRATION
import com.labelbox.adv.retrofill.dao.DatasetOnSameOrgDetector.Companion.PARTIAL_WITHOUT_ATTR_MIGRATION
import org.slf4j.LoggerFactory
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobExecutionListener
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.launch.NoSuchJobException
import org.springframework.batch.core.repository.JobRepository
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.support.SynchronizedItemStreamReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.TaskExecutor
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import javax.sql.DataSource

@Configuration
class DatasetLevelMigrationBatchJobConfig : DefaultBatchConfiguration() {

    @Autowired
    @Qualifier("h2DataSource")
    lateinit var h2DataSource: DataSource

    @Autowired
    @Qualifier("jobDataSource")
    lateinit var jobDataSource: DataSource

    @Autowired
    lateinit var storeMutationWriter: ItemWriter<Mutation>

    @Autowired
    lateinit var storeListOfMutationWriter: ItemWriter<List<Mutation>>

    @Autowired
    lateinit var slackJobExecutionListener: JobExecutionListener

    @Autowired
    @Qualifier("synchronizedStoreDatasetReaderWithDatasetIds")
    lateinit var datasetReader: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedDataRowReaderWithDatasetIds")
    lateinit var dataRowReader: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedAttachmentReaderWithDatasetIds")
    lateinit var attachmentReader: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedMetadataReaderWithDatasetIds")
    lateinit var metadataRowReader: SynchronizedItemStreamReader<List<Mutation>>

    @Autowired
    lateinit var detector: DatasetOnSameOrgDetector

    fun executeJob(jobParameters: JobParameters): JobExecution {
        val je = jobLauncher().run(datasetMigrationJob(jobRepository()), jobParameters)
        return je
    }

    override fun getTaskExecutor(): TaskExecutor {
        return SimpleAsyncTaskExecutor()
    }

    @Bean
    fun datasetMigrationJob(jobRepository: JobRepository): Job {
        return JobBuilder(JOB_NAME, jobRepository())
            .start(datasetDeciderFlow())
            .end()
            .listener(slackJobExecutionListener)
            .build()
    }

    @Bean
    fun datasetDeciderFlow(): Flow {
        return FlowBuilder<Flow>("adv_dataset_migration_decider_flow")
            .start(detector)
            .on(FULL_MIGRATION).to(getMetaDataMigrateFlow())
            .from(detector).on(PARTIAL_MIGRATION).to(getMetaDataMigrateFlow())
            .from(detector).on(PARTIAL_WITHOUT_ATTR_MIGRATION).to(getMetaDataMigrateFlow())
            .from(detector).on("*").end()
            .build()
    }

    fun getMetaDataMigrateFlow(skipDataRow: Boolean = false, skipAttachment: Boolean = false): Flow {
        if (!skipDataRow) {
            return FlowBuilder<Flow>("full_migration_flow")
                .start(migrateDataSet())
                .next(getDataRowMigrationStep())
                .next(getAttachmentMigrationStep())
                .next(getMetadataMigrationStep())
                .build()
        } else {
            return if (skipAttachment) {
                FlowBuilder<Flow>("metadata_only_migration_flow")
                    .start(migrateDataSet())
                    .next(getMetadataMigrationStep())
                    .build()
            } else {
                FlowBuilder<Flow>("partial_migration_flow")
                    .start(migrateDataSet())
                    .next(getAttachmentMigrationStep())
                    .next(getMetadataMigrationStep())
                    .build()
            }
        }
    }

    /* Migration Steps */
    fun migrateDataSet(): Step {
        return StepBuilder("migrateDataSet", jobRepository())
            .chunk<Mutation, Mutation>(500, transactionManager)
            .reader(datasetReader)
            .writer(storeMutationWriter)
            .taskExecutor(threadedTaskExecutorForDatasetMigration())
            .build()
    }

    fun getDataRowMigrationStep(): Step {
        return StepBuilder("migrateDataRow", jobRepository())
            .chunk<Mutation, Mutation>(1000, transactionManager)
            .reader(dataRowReader)
            .writer(storeMutationWriter)
            .taskExecutor(threadedTaskExecutorForDatasetMigration())
            .faultTolerant()
            .skip(com.google.cloud.spanner.SpannerException::class.java)
            .skipLimit(FAULT_TOLERANCE_ROWS)
            .build()
    }

    fun getAttachmentMigrationStep(): Step {
        return StepBuilder("migrateAttachments", jobRepository())
            .chunk<Mutation, Mutation>(1000, transactionManager)
            .reader(attachmentReader)
            .writer(storeMutationWriter)
            .taskExecutor(threadedTaskExecutorForDatasetMigration())
            .faultTolerant()
            .skip(com.google.cloud.spanner.SpannerException::class.java)
            .skipLimit(FAULT_TOLERANCE_ROWS)
            .build()
    }

    fun getMetadataMigrationStep(): Step {
        return StepBuilder("migrateMetadata", jobRepository())
            .chunk<List<Mutation>, List<Mutation>>(500, transactionManager)
            .reader(metadataRowReader)
            .writer(storeListOfMutationWriter)
            .taskExecutor(threadedTaskExecutorForDatasetMigration())
            .faultTolerant()
            .skip(com.google.cloud.spanner.SpannerException::class.java)
            .skipLimit(FAULT_TOLERANCE_ROWS)
            .build()
    }

    override fun getDataSource(): DataSource {
        return jobDataSource
    }

    override fun getTransactionManager(): PlatformTransactionManager {
        return getDatasetMigrationTransactionManager()
    }

    @Bean
    fun getDatasetMigrationTransactionManager(): PlatformTransactionManager {
        return DataSourceTransactionManager(jobDataSource)
    }

    /*Task executor*/
    @Bean
    fun threadedTaskExecutorForDatasetMigration(): TaskExecutor {
        val taskExecutor = SimpleAsyncTaskExecutor()
        taskExecutor.concurrencyLimit = NUMBER_OF_WRITER_THREAD
        return taskExecutor
    }

    fun getNumRunningJobs(): Int {
        return try {
            val num = jobOperator().getRunningExecutions(JOB_NAME).size
            num
        } catch (e: NoSuchJobException) {
            0
        }
    }

    companion object {
        val JOB_NAME = "adv_migration_on_dataset_ids"
        val FAULT_TOLERANCE_ROWS = 10
        private val logger = LoggerFactory.getLogger(this::class.java)
        private const val NUMBER_OF_WRITER_THREAD = 16
    }
}
