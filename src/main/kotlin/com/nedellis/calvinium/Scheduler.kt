package com.nedellis.calvinium

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

private class LockManager() {
    private val lock = ReentrantLock()
    private val pendingLeaseStatusByTxnUUID =
        mutableMapOf<UUID, MutableStateFlow<ImmutableSet<RecordKey>>>()
    private val pendingLeaseOwnerByRecordKey = mutableMapOf<RecordKey, ImmutableList<UUID>>()

    private fun acquireRecordLocksFlow(
        txnUUID: UUID,
        sortedKeys: List<RecordKey>
    ): StateFlow<ImmutableSet<RecordKey>> {
        lock.withLock {
            val leaseStatusFlow =
                pendingLeaseStatusByTxnUUID.getOrPut(txnUUID) {
                    MutableStateFlow(ImmutableSet.of())
                }
            for (recordKey in sortedKeys) {
                val pendingLeaseOwners =
                    pendingLeaseOwnerByRecordKey.getOrDefault(recordKey, ImmutableList.of())

                val newPendingLeaseOwners =
                    ImmutableList.builder<UUID>().addAll(pendingLeaseOwners).add(txnUUID).build()

                pendingLeaseOwnerByRecordKey[recordKey] = newPendingLeaseOwners

                if (newPendingLeaseOwners.first() == txnUUID) {
                    leaseStatusFlow.update { readyRecordKeys ->
                        ImmutableSet.builder<RecordKey>()
                            .addAll(readyRecordKeys)
                            .add(recordKey)
                            .build()
                    }
                }
            }

            return leaseStatusFlow.asStateFlow()
        }
    }

    private fun releaseRecordLocks(txnUUID: UUID, sortedKeys: List<RecordKey>) {
        lock.withLock {
            for (recordKey in sortedKeys) {
                val pendingLeaseOwners =
                    pendingLeaseOwnerByRecordKey.getOrDefault(recordKey, ImmutableList.of())

                assert(pendingLeaseOwners.first() == txnUUID)

                val newPendingLeaseOwners =
                    ImmutableList.builder<UUID>().addAll(pendingLeaseOwners.drop(1)).build()

                if (newPendingLeaseOwners.isEmpty()) {
                    pendingLeaseOwnerByRecordKey.remove(recordKey)
                } else {
                    pendingLeaseOwnerByRecordKey[recordKey] = newPendingLeaseOwners

                    // Tell the next transaction it can start running
                    val leaseStatusFlowForNewOwner =
                        pendingLeaseStatusByTxnUUID[newPendingLeaseOwners.first()]!!
                    leaseStatusFlowForNewOwner.update { readyRecordKeys ->
                        ImmutableSet.builder<RecordKey>()
                            .addAll(readyRecordKeys)
                            .add(recordKey)
                            .build()
                    }
                }
            }

            pendingLeaseStatusByTxnUUID.remove(txnUUID)
        }
    }

    suspend fun <T : Any> withLocks(txnUUID: UUID, sortedKeys: List<RecordKey>, body: () -> T): T {
        val leaseStatusFlow = acquireRecordLocksFlow(txnUUID, sortedKeys)

        leaseStatusFlow.filter { readyRecordKeys -> readyRecordKeys == sortedKeys.toSet() }.first()

        try {
            return body()
        } finally {
            releaseRecordLocks(txnUUID, sortedKeys)
        }
    }
}

class Scheduler(private val executor: Executor) {
    private val logger = KotlinLogging.logger {}
    private val lm = LockManager()

    suspend fun run(uniqueTxn: UniqueTransaction): RecordValue {
        val keys = uniqueTxn.txn.operations.map { it.key }
        logger.debug { "Scheduler starting TXN: $uniqueTxn" }
        return lm.withLocks(uniqueTxn.id, keys.sorted()) { runBlocking { executor.run(uniqueTxn) } }
    }
}
