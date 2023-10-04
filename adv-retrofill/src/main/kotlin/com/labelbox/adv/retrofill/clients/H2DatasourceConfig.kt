package com.labelbox.adv.retrofill.clients

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType
import javax.sql.DataSource

@Configuration
class H2DatasourceConfig {
    @Bean
    fun h2DataSource(): DataSource {
        return EmbeddedDatabaseBuilder()
            .setType(EmbeddedDatabaseType.H2)
            .addScript("org/springframework/batch/core/schema-drop-h2.sql")
            .addScript("org/springframework/batch/core/schema-h2.sql")
            .build()
    }
}
