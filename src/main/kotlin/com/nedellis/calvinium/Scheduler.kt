package com.nedellis.calvinium

import com.google.common.util.concurrent.Striped
import kotlinx.coroutines.runBlocking

private class OldLockManager() {
    // TODO: Convert to Mutex()
    private val leases = Striped.lazyWeakLock(NUM_STRIPES)

    fun <T : Any> withLocks(keys: List<RecordKey>, body: () -> T): T {
        val sortedLocks = keys.sorted().map { leases.get(it) }
        sortedLocks.forEach { it.lock() }
        try {
            return body()
        } finally {
            sortedLocks.forEach { it.unlock() }
        }
    }

    companion object {
        private const val NUM_STRIPES = 1024
    }
}

class Scheduler(val executor: Executor) {
    private val lm = OldLockManager()

    fun run(uniqueTxn: UniqueTransaction): RecordValue {
        val keys = uniqueTxn.txn.operations.map { it.key }
        return lm.withLocks(keys) { runBlocking { executor.run(uniqueTxn) } }
    }
}