package com.labelbox.adv.retrofill.clients

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.net.URL

@Configuration
class ElasticsearchClientConfig {

    @Bean
    fun elasticsearchClient(
        @Value("\${spring.es.url}") url: String,
        credentialsProvider: CredentialsProvider
    ): ElasticsearchClient {
        val esUrl = URL(url)
        logger.info("Creating elasticsearch client to: $url")

        return ElasticsearchClient(
            elasticRestClientTransport(esUrl, credentialsProvider)
        )
    }

    @Bean
    fun elasticCredentialsProvider(
        @Value("\${spring.es.username}") username: String?,
        @Value("\${spring.es.password}") password: String?
    ): CredentialsProvider {
        val credentialsProvider: CredentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            UsernamePasswordCredentials(username, password)
        )
        return credentialsProvider
    }

    fun elasticRestClientTransport(url: URL, credentialsProvider: CredentialsProvider): RestClientTransport {
        val restClient = RestClient.builder(HttpHost(url.host, url.port, url.protocol))
            .setHttpClientConfigCallback { httpClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(
                    credentialsProvider
                )
            }.build()

        return RestClientTransport(
            restClient,
            JacksonJsonpMapper()
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }
}
