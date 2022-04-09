package com.nedellis.calvinium

import com.google.common.util.concurrent.Striped
import java.util.UUID

private class LockManager() {
    //    private val leases: MutableMap<String, UUID> = mutableMapOf()
    private val leases = Striped.lazyWeakLock(NUM_STRIPES)

    fun <T> withLocks(keys: List<String>, body: () -> T): T {
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
    private val lm = LockManager()

    fun run(uniqueTxn: UniqueTransaction): String? {
        val keys = uniqueTxn.txn.operations.map { it.key }
        return lm.withLocks(keys) {
            executor.run(uniqueTxn)
        }
    }
}