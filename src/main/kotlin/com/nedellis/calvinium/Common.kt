package com.nedellis.calvinium

import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import java.security.MessageDigest
import java.util.UUID

data class Transaction(val operations: List<Operation>)

data class Operation(val key: RecordKey, val type: OperationType)

data class RecordKey(val tableId: Long, val keyId: Long) : Comparable<RecordKey> {
    fun toBytes(): ByteArray {
        return Shorts.toByteArray(virtualNodeId())
            .plus(Longs.toByteArray(tableId))
            .plus(Longs.toByteArray(keyId))
    }

    fun virtualNodeId(): Short {
        val digest =
            MessageDigest.getInstance("MD5")
                .digest(Longs.toByteArray(tableId).plus(Longs.toByteArray(keyId)))!!
        return Shorts.fromBytes(digest[0], digest[1])
    }

    companion object {
        const val VIRTUAL_NODE_BYTES = 2
    }

    override fun compareTo(other: RecordKey): Int {
        return when {
            (tableId == other.tableId) -> (keyId - other.keyId).toInt()
            else -> (tableId - other.tableId).toInt()
        }
    }
}

/**
 * This is a data class so its obvious when the record value is part of a transaction but currently
 * empty
 */
data class RecordValue(val contents: String? = null)

sealed interface OperationType

data class Put(val value: String) : OperationType

object Get : OperationType

object Delete : OperationType

data class UniqueTransaction(val id: UUID, val txn: Transaction)
