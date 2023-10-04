package com.labelbox.adv.retrofill.batching

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.Mutation
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.DETECT_FULL
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.DETECT_FULL_TEST
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.DETECT_PARTIAL
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.DETECT_PARTIAL_TEST
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.DETECT_PARTIAL_WITHOUT_ATTR
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.DETECT_PARTIAL_WITHOUT_ATTR_TEST
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.PASS_FULL
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.PASS_FULL_TEST
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.PASS_PARTIAL
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.PASS_PARTIAL_TEST
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.PASS_PARTIAL_WITHOUT_ATTR
import com.labelbox.adv.retrofill.dao.DanglingDataRowDetector.Companion.PASS_PARTIAL_WITHOUT_ATTR_TEST
import com.labelbox.adv.retrofill.dao.EsIncrementalUpdater
import com.labelbox.adv.retrofill.dao.IncrementalUpdateDetector
import com.labelbox.adv.retrofill.dao.SkipExecutionListener
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
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.SimpleAsyncTaskExecutor
import org.springframework.core.task.TaskExecutor
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import javax.sql.DataSource

@Configuration
class OrgLevelMigrationBatchJobConfig : DefaultBatchConfiguration() {

    @Autowired
    @Qualifier("h2DataSource")
    lateinit var h2DataSource: DataSource

    @Autowired
    @Qualifier("jobDataSource")
    lateinit var jobDataSource: DataSource

    @Autowired
    lateinit var spannerDatabaseClient: DatabaseClient

    @Autowired
    @Qualifier("storeMutationWriter")
    lateinit var storeMutationWriter: ItemWriter<Mutation>

    @Autowired
    @Qualifier("storeListOfMutationWriter")
    lateinit var storeListOfMutationWriter: ItemWriter<List<Mutation>>

    @Autowired
    @Qualifier("storeMutationTestWriter")
    lateinit var storeMutationTestWriter: ItemWriter<Mutation>

    @Autowired
    @Qualifier("storeListOfMutationTestWriter")
    lateinit var storeListOfMutationTestWriter: ItemWriter<List<Mutation>>

    @Autowired
    lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    lateinit var slackJobExecutionListener: JobExecutionListener

    @Autowired
    lateinit var LDJobExecutionListener: JobExecutionListener

    @Autowired
    lateinit var detector: DanglingDataRowDetector

    @Autowired
    lateinit var incrementalUpdateDetector: IncrementalUpdateDetector

    @Autowired
    lateinit var esIncrementalUpdater: EsIncrementalUpdater

    @Autowired
    @Qualifier("synchronizedStoreDatasetReader")
    lateinit var datasetReader: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedDataRowReaderWithDanglingRows")
    lateinit var rowReaderWithDanglingRows: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedDataRowReader")
    lateinit var rowReader: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedAttachmentReaderWithDanglingRows")
    lateinit var attachmentReaderWithDanglingRows: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedAttachmentReader")
    lateinit var attachmentReader: SynchronizedItemStreamReader<Mutation>

    @Autowired
    @Qualifier("synchronizedMetedataReaderWithDanglingRows")
    lateinit var metadataRowReaderWithDanglingRows: SynchronizedItemStreamReader<List<Mutation>>

    @Autowired
    @Qualifier("synchronizedMetadataReader")
    lateinit var metadataRowReader: SynchronizedItemStreamReader<List<Mutation>>

    @Autowired
    lateinit var skipExecutionListener: SkipExecutionListener

    fun executeJob(jobParameters: JobParameters): JobExecution {
        val je = jobLauncher().run(orgsMigrationJob(jobRepository()), jobParameters)
        return je
    }

    override fun getTaskExecutor(): TaskExecutor {
        return SimpleAsyncTaskExecutor()
    }

    data class Quadruple<out A, out B, out C, out D>(
        val first: A,
        val second: B,
        val third: C,
        val fourth: D
    )

    @Bean
    fun orgsMigrationJob(jobRepository: JobRepository): Job {
        return JobBuilder(JOB_NAME, jobRepository())
            .start(startStep())
            .next(detector).on(PASS_FULL).to(getMigrateFlow(hasDanglingRows = false))
            .from(detector).on(PASS_PARTIAL).to(getMigrateFlow(hasDanglingRows = false, skipDataRow = true))
            .from(detector).on(PASS_PARTIAL_WITHOUT_ATTR).to(getMigrateFlow(hasDanglingRows = false, skipDataRow = true, skipAttachment = true))
            .from(detector).on(DETECT_FULL).to(getMigrateFlow(hasDanglingRows = true))
            .from(detector).on(DETECT_PARTIAL).to(getMigrateFlow(hasDanglingRows = true, skipDataRow = true))
            .from(detector).on(DETECT_PARTIAL_WITHOUT_ATTR).to(getMigrateFlow(hasDanglingRows = true, skipDataRow = true, skipAttachment = true))
            .from(detector).on(PASS_FULL_TEST).to(getMigrateFlow(hasDanglingRows = false, isTestRun = true))
            .from(detector).on(PASS_PARTIAL_TEST).to(getMigrateFlow(hasDanglingRows = false, skipDataRow = true, isTestRun = true))
            .from(detector).on(PASS_PARTIAL_WITHOUT_ATTR_TEST).to(getMigrateFlow(hasDanglingRows = false, skipDataRow = true, skipAttachment = true, isTestRun = true))
            .from(detector).on(DETECT_FULL_TEST).to(getMigrateFlow(hasDanglingRows = true, isTestRun = true))
            .from(detector).on(DETECT_PARTIAL_TEST).to(getMigrateFlow(hasDanglingRows = true, skipDataRow = true, isTestRun = true))
            .from(detector).on(DETECT_PARTIAL_WITHOUT_ATTR_TEST).to(getMigrateFlow(hasDanglingRows = true, skipDataRow = true, skipAttachment = true, isTestRun = true))
            .end()
            .listener(slackJobExecutionListener)
            .listener(LDJobExecutionListener)
            .build()
    }

    fun getMigrateFlowCopu(hasDanglingRows: Boolean, skipDataRow: Boolean = false, isTestRun: Boolean = false): Flow {
        val flowName = if (hasDanglingRows) "migration_flow_with_dangling_rows" else "migration_flow_without_dangling_rows"
        val (dsStep, drStep, attStep, metaStep) = getMigrationSteps(hasDanglingRows, isTestRun)
        return if (skipDataRow) {
            FlowBuilder<Flow>(flowName)
                .start(dsStep)
                .next(attStep)
                .next(metaStep)
                .next(esIncrementalUpdater).on("*").to(getEndStep())
                .build()
        } else {
            FlowBuilder<Flow>(flowName)
                .start(dsStep)
                .next(drStep)
                .next(attStep)
                .next(metaStep)
                .next(esIncrementalUpdater).on("*").to(getEndStep())
                .build()
        }
    }

    fun getMigrateFlow(hasDanglingRows: Boolean, skipDataRow: Boolean = false, skipAttachment: Boolean = false, isTestRun: Boolean = false): Flow {
        val flowName = if (hasDanglingRows) "migration_flow_with_dangling_rows" else "migration_flow_without_dangling_rows"
        val (dsStep, drStep, attStep, metaStep) = getMigrationSteps(hasDanglingRows, isTestRun)
        return if (skipDataRow) {
            if (skipAttachment) {
                FlowBuilder<Flow>(flowName)
                    .start(dsStep)
                    .next(metaStep)
                    .next(esIncrementalUpdater).on("*").to(getEndStep())
                    .build()
            } else {
                FlowBuilder<Flow>(flowName)
                    .start(dsStep)
                    .next(attStep)
                    .next(metaStep)
                    .next(esIncrementalUpdater).on("*").to(getEndStep())
                    .build()
            }
        } else {
            FlowBuilder<Flow>(flowName)
                .start(dsStep)
                .next(drStep)
                .next(attStep)
                .next(metaStep)
                .next(esIncrementalUpdater).on("*").to(getEndStep())
                .build()
        }
    }

    fun getMigrationSteps(hasDanglingRows: Boolean, isTestRun: Boolean = false): Quadruple<Step, Step, Step, Step> {
        val mutationWriter = if (isTestRun) storeMutationTestWriter else storeMutationWriter
        val listMutationWriter = if (isTestRun) storeListOfMutationTestWriter else storeListOfMutationWriter
        val datasetMigrationStep =
            StepBuilder("migrateDataSet", jobRepository())
                .chunk<Mutation, Mutation>(500, transactionManager)
                .reader(datasetReader)
                .writer(mutationWriter)
                .taskExecutor(threadedTaskExecutorForOrgsMigration())
                .build()

        val dataRowMigrationStep =
            StepBuilder("migrateDataRow", jobRepository())
                .chunk<Mutation, Mutation>(1000, transactionManager)
                .reader(if (hasDanglingRows) rowReaderWithDanglingRows else rowReader)
                .writer(mutationWriter)
                .taskExecutor(threadedTaskExecutorForOrgsMigration())
                .faultTolerant()
                .skip(com.google.cloud.spanner.SpannerException::class.java)
                .skipLimit(FAULT_TOLERANCE_ROWS)
                .listener(skipExecutionListener)
                .build()

        val attachmentMigrationStep =
            StepBuilder("migrateAttachments", jobRepository())
                .chunk<Mutation, Mutation>(1000, transactionManager)
                .reader(if (hasDanglingRows) attachmentReaderWithDanglingRows else attachmentReader)
                .writer(mutationWriter)
                .taskExecutor(threadedTaskExecutorForOrgsMigration())
                .faultTolerant()
                .skip(com.google.cloud.spanner.SpannerException::class.java)
                .skipLimit(FAULT_TOLERANCE_ROWS)
                .build()

        val metaDataMigrationStep =
            StepBuilder("migrateMetadata", jobRepository())
                .chunk<List<Mutation>, List<Mutation>>(500, transactionManager)
                .reader(if (hasDanglingRows) metadataRowReaderWithDanglingRows else metadataRowReader)
                .writer(listMutationWriter)
                .taskExecutor(threadedTaskExecutorForOrgsMigration())
                .faultTolerant()
                .skip(com.google.cloud.spanner.SpannerException::class.java)
                .skipLimit(FAULT_TOLERANCE_ROWS)
                .build()
        return Quadruple(datasetMigrationStep, dataRowMigrationStep, attachmentMigrationStep, metaDataMigrationStep)
    }

    fun getEndStep(): Step = StepBuilder("endStep", jobRepository())
        .tasklet({ _, _ -> RepeatStatus.FINISHED }, transactionManager)
        .build()

    @Bean
    override fun getDataSource(): DataSource {
        return jobDataSource
    }

    /* Migration Steps */
    @Bean
    fun startStep(): Step {
        return StepBuilder("startMigration", jobRepository())
            .tasklet({ _, _ -> RepeatStatus.FINISHED }, transactionManager)
            .build()
    }

    override fun getTransactionManager(): PlatformTransactionManager {
        return getOrgsMigrationTransactionManager()
    }

    @Bean
    fun getOrgsMigrationTransactionManager(): PlatformTransactionManager {
        return DataSourceTransactionManager(jobDataSource)
    }

    /*Task executor*/
    @Bean
    fun threadedTaskExecutorForOrgsMigration(): TaskExecutor {
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
        val JOB_NAME = "adv_migration_on_org_ids"
        val FAULT_TOLERANCE_ROWS = 50
        private val logger = LoggerFactory.getLogger(this::class.java)
        private const val NUMBER_OF_WRITER_THREAD = 16
    }
}
