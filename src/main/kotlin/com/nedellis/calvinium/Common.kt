package com.nedellis.calvinium

import com.google.common.primitives.Longs
import java.security.MessageDigest
import java.util.UUID

data class Transaction(val operations: List<Operation>)
data class Operation(val key: RecordKey, val type: OperationType)

data class RecordKey(val tableId: Long, val keyId: Long) : Comparable<RecordKey> {
    fun toBytes(): ByteArray {
        val digest =
            MessageDigest.getInstance("MD5").digest(Longs.toByteArray(tableId).plus(Longs.toByteArray(keyId)))!!
        return digest.take(VIRTUAL_NODE_BYTES).toByteArray()
            .plus(Longs.toByteArray(tableId))
            .plus(Longs.toByteArray(keyId))
    }

    companion object {
        const val VIRTUAL_NODE_BYTES = 2
    }

    override fun compareTo(other: RecordKey): Int {
        if (tableId > other.tableId) {
            return 1
        } else if (tableId < other.tableId) {
            return -1
        } else {
            if (keyId > other.keyId) {
                return 1
            } else if (keyId < other.keyId) {
                return -1
            } else {
                return 0
            }
        }
    }
}

sealed interface OperationType
data class Put(val value: String) : OperationType
object Get : OperationType
object Delete : OperationType

data class UniqueTransaction(val id: UUID, val txn: Transaction)