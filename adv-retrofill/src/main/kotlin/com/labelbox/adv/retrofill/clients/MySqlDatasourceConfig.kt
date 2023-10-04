package com.labelbox.adv.retrofill.clients

import org.apache.commons.dbcp2.BasicDataSource
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.stereotype.Component
import java.util.*
import javax.sql.DataSource

@Component
@ConfigurationProperties(prefix = "spring.datasource")
class MySqlConfig {
    lateinit var host: String
    lateinit var port: String
    lateinit var database: String
    lateinit var username: String
    lateinit var password: String
    lateinit var replica_port: String
    lateinit var job_database: String
}

@Configuration
class MySqlDatasourceConfig {
    private val driverClassName = "com.mysql.cj.jdbc.Driver"
    private val queryTimeoutSetting = "sessionVariables=MAX_EXECUTION_TIME=2000000"

    @Bean
    fun mysqlDataSource(
        config: MySqlConfig
    ): DataSource {
        val url = "jdbc:mysql://${config.host}:${config.replica_port}/${config.database}?$queryTimeoutSetting"
        val ds =
            DriverManagerDataSource(
                url,
                config.username,
                config.password
            )
        ds.setDriverClassName(driverClassName)
        return ds
    }

    @Bean
    fun jobDataSource(
        config: MySqlConfig
    ): DataSource {
        val ds = BasicDataSource()
        ds.url = "jdbc:mysql://${config.host}:${config.port}/${config.job_database}?$queryTimeoutSetting"
        ds.username = config.username
        ds.password = config.password
        ds.driverClassName = driverClassName
        ds.initialSize = 4
        ds.maxTotal = 16
        ds.defaultQueryTimeout = 36_000
        ds.maxWaitMillis = 36_000_000
        return ds
    }

    @Bean
    fun jdbcTemplate(
        @Qualifier("mysqlDataSource")
        mysqlDataSource: DataSource
    ): JdbcTemplate {
        return JdbcTemplate(mysqlDataSource)
    }
}
