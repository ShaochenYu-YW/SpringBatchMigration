package com.labelbox.adv.retrofill.batching

import com.google.cloud.spanner.Mutation
import com.labelbox.adv.retrofill.dao.DataRowOnSameOrgDetector
import com.labelbox.adv.retrofill.dao.DataRowOnSameOrgDetector.Companion.PASS_DETECTION
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
class DataRowLevelMigrationBatchJobConfig : DefaultBatchConfiguration() {

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
    @Qualifier("synchronizedDataRowReaderWithDataRowIds")
    lateinit var dataRowReader: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedAttachmentReaderWithDataRowIds")
    lateinit var attachmentReader: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedMetadataReaderWithDataRowIds")
    lateinit var metadataRowReader: SynchronizedItemStreamReader<List<Mutation>>

    @Autowired
    lateinit var detector: DataRowOnSameOrgDetector

    fun executeJob(jobParameters: JobParameters): JobExecution {
        val je = jobLauncher().run(dataRowMigrationJob(jobRepository()), jobParameters)
        return je
    }

    override fun getTaskExecutor(): TaskExecutor {
        return SimpleAsyncTaskExecutor()
    }

    @Bean
    fun dataRowMigrationJob(jobRepository: JobRepository): Job {
        return JobBuilder(JOB_NAME, jobRepository())
            .start(dataRowDeciderFlow())
            .end()
            .listener(slackJobExecutionListener)
            .build()
    }

    @Bean
    fun dataRowDeciderFlow(): Flow {
        return FlowBuilder<Flow>("adv_data_row_migration_decider_flow")
            .start(detector)
            .on(PASS_DETECTION).to(getMigrateFlow())
            .from(detector).on("*").end()
            .build()
    }

    fun getMigrateFlow(): Flow {
        return FlowBuilder<Flow>("data_row_migration_flow")
            .start(getDataRowMigrationStep())
            .next(getAttachmentMigrationStep())
            .next(getMetadataMigrationStep())
            .build()
    }

    fun getDataRowMigrationStep(): Step {
        return StepBuilder("migrateDataRow", jobRepository())
            .chunk<Mutation, Mutation>(1000, transactionManager)
            .reader(dataRowReader)
            .writer(storeMutationWriter)
            .build()
    }

    fun getAttachmentMigrationStep(): Step {
        return StepBuilder("migrateAttachments", jobRepository())
            .chunk<Mutation, Mutation>(500, transactionManager)
            .reader(attachmentReader)
            .writer(storeMutationWriter)
            .build()
    }

    fun getMetadataMigrationStep(): Step {
        return StepBuilder("migrateMetadata", jobRepository())
            .chunk<List<Mutation>, List<Mutation>>(500, transactionManager)
            .reader(metadataRowReader)
            .writer(storeListOfMutationWriter)
            .build()
    }

    override fun getDataSource(): DataSource {
        return jobDataSource
    }

    override fun getTransactionManager(): PlatformTransactionManager {
        return getDataRowMigrationTransactionManager()
    }

    @Bean
    fun getDataRowMigrationTransactionManager(): PlatformTransactionManager {
        return DataSourceTransactionManager(jobDataSource)
    }

    companion object {
        val JOB_NAME = "adv_migration_on_data_row_ids"
        private val logger = LoggerFactory.getLogger(this::class.java)
    }
}
