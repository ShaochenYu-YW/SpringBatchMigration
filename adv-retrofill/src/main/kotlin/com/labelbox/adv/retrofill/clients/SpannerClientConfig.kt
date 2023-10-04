package com.labelbox.adv.retrofill.clients

import com.google.cloud.spanner.DatabaseClient
import com.google.cloud.spanner.DatabaseId
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.SessionPoolOptions
import com.google.cloud.spanner.Spanner
import com.google.cloud.spanner.SpannerOptions
import org.springframework.batch.item.Chunk
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SpannerClientConfig {

    @Bean
    fun spannerDatabaseClient(
        @Value("\${spanner.url}") url: String
    ): DatabaseClient {
        val sessionPoolOptions = SessionPoolOptions.newBuilder()
            .setKeepAliveIntervalMinutes(59)
            .build()
        val options: SpannerOptions = SpannerOptions.newBuilder().setNumChannels(16).setSessionPoolOption(sessionPoolOptions).build()
        val spanner: Spanner = options.service
        val databaseId: DatabaseId = DatabaseId.of(url)
        return spanner.getDatabaseClient(databaseId)
    }

    @Bean
    fun testSpannerDatabaseClient(
        @Value("\${spanner.test-source-url}") url: String
    ): DatabaseClient {
        val sessionPoolOptions = SessionPoolOptions.newBuilder()
            .setKeepAliveIntervalMinutes(59)
            .build()
        val options: SpannerOptions = SpannerOptions.newBuilder().setNumChannels(16).setSessionPoolOption(sessionPoolOptions).build()
        val spanner: Spanner = options.service
        val databaseId: DatabaseId = DatabaseId.of(url)
        return spanner.getDatabaseClient(databaseId)
    }

    @Bean
    fun storeMutationWriter(
        spannerDatabaseClient: DatabaseClient
    ): ItemWriter<Mutation> {
        return ItemWriter { items: Chunk<out Mutation> ->
            spannerDatabaseClient.write(items)
        }
    }

    @Bean
    fun storeListOfMutationWriter(
        spannerDatabaseClient: DatabaseClient
    ): ItemWriter<List<Mutation>> {
        return ItemWriter { items: Chunk<out List<Mutation>> ->
            val flattenedMutations = items.flatMap { it }
            spannerDatabaseClient.write(flattenedMutations)
        }
    }

    @Bean
    fun storeMutationTestWriter(
        testSpannerDatabaseClient: DatabaseClient
    ): ItemWriter<Mutation> {
        return ItemWriter { items: Chunk<out Mutation> ->
            testSpannerDatabaseClient.write(items)
        }
    }

    @Bean
    fun storeListOfMutationTestWriter(
        testSpannerDatabaseClient: DatabaseClient
    ): ItemWriter<List<Mutation>> {
        return ItemWriter { items: Chunk<out List<Mutation>> ->
            val flattenedMutations = items.flatMap { it }
            testSpannerDatabaseClient.write(flattenedMutations)
        }
    }
}
