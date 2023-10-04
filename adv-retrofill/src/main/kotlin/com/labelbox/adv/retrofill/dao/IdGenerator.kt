package com.labelbox.adv.retrofill.dao

import org.apache.commons.codec.digest.MurmurHash3

/**
 * A utility for generating various froms of IDs or random strings.
 */
object IdGenerator {
    private const val SYMBOLS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0987654321"

    fun murmurHash32(value: String?): Int? {
        return if (value == null) {
            null
        } else {
            MurmurHash3.hash32x86(value.toByteArray())
        }
    }
}
