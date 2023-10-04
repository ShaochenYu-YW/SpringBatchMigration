package com.labelbox.adv.retrofill.domain

import kotlin.math.abs

enum class AttachmentType {
    RAW_TEXT,
    TEXT_URL,
    IMAGE,
    VIDEO,
    HTML,
    IMAGE_OVERLAY
    ;

    companion object {
        private val stringToEnumMap = mapOf(
            "RAW_TEXT" to RAW_TEXT,
            "TEXT_URL" to TEXT_URL,
            "IMAGE" to IMAGE,
            "VIDEO" to VIDEO,
            "HTML" to HTML,
            "IMAGE_OVERLAY" to IMAGE_OVERLAY,
            "TEXT" to RAW_TEXT
        )

        fun fromString(value: String): AttachmentType? {
            return stringToEnumMap[value]
        }
    }
}

enum class MetadataType {
    NUMBER,
    STRING,
    DATE,
    ENUM,
    GPS_POINT
    ;

    companion object {
        private val stringToEnumMap = mapOf(
            "CustomMetadataString" to STRING,
            "CustomMetadataDateTime" to DATE,
            "CustomMetadataNumber" to NUMBER,
            "CustomMetadataEnumOption" to ENUM,
            "CustomMetadataGpsPoint" to GPS_POINT
        )

        fun fromString(value: String): MetadataType? {
            return stringToEnumMap[value]
        }
    }
}

object ApiUser {
    val userId = "clfma5cac00003b6ir76m7ohe"
}

class ShardRange(
    val first: Int,
    val last: Int
) {
    constructor() : this(1, MAX_SHARDS)

    companion object {
        // Maximum number of shards.
        const val MAX_SHARDS = 64

        fun getShardKey(value: String): Int {
            return abs(value.hashCode() % MAX_SHARDS) + 1
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ShardRange

        if (first != other.first) return false
        if (last != other.last) return false

        return true
    }

    override fun hashCode(): Int {
        var result = first
        result = 31 * result + last
        return result
    }

    override fun toString(): String {
        return "ShardRange(first=$first, last=$last)"
    }
}
