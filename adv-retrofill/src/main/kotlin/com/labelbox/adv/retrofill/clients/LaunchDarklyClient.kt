package com.labelbox.adv.retrofill.clients

import okhttp3.Headers.Companion.toHeaders
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "spring.ld.api")
class LDClientConfiguration {
    lateinit var url: String
    lateinit var project: String
    lateinit var environment: String
    lateinit var in_migration_segment: String
    lateinit var segment: String
    lateinit var api_token: String
}

@Configuration
class LaunchDarklyClient(config: LDClientConfiguration) {
    val url = "${config.url}/${config.project}/${config.environment}/${config.segment}"
    val in_migration_url = "${config.url}/${config.project}/${config.environment}/${config.in_migration_segment}"

    val headers = mapOf(
        "Content-Type" to "application/json",
        "Authorization" to "${config.api_token}"
    )

    fun patchToDualWrite(orgids: List<String>): Response {
        return makeAddRequest(orgids, in_migration_url)
    }

    fun patch(orgids: List<String>): Response {
        return makeAddRequest(orgids, url)
    }

    fun makeAddRequest(orgIds: List<String>, url: String): Response {
        val client = OkHttpClient()
        val patchArray = JSONArray()
        for (orgId in orgIds) {
            val patchObject = JSONObject()
                .put("op", "add")
                .put("path", "/rules/0/clauses/0/values/0")
                .put("value", orgId)
            patchArray.put(patchObject)
        }

        val payload = JSONObject().put("patch", patchArray)
        val mediaType = "application/json".toMediaType()
        val body = payload.toString().toRequestBody(mediaType)

        val req = Request.Builder()
            .url(url)
            .headers(headers.toHeaders())
            .patch(body)
            .build()
        return client.newCall(req).execute().use { response ->
            response
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }
}
