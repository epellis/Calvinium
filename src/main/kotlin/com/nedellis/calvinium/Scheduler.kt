package com.nedellis.calvinium

import com.google.common.util.concurrent.Striped
import java.util.UUID

private class LockManager() {
    //    private val leases: MutableMap<String, UUID> = mutableMapOf()
    private val leases = Striped.lazyWeakLock(NUM_STRIPES)

    inline fun <T> withLock(key: String, body: () -> T): T {
        val lock = leases.get(key)
        lock.lock()
        try {
            return body()
        } finally {
            lock.unlock()
        }
    }

    companion object {
        private const val NUM_STRIPES = 1024
    }
}

class Scheduler(val executor: Executor) {
    private val lm = LockManager()

    fun run(uniqueOp: UniqueOperation): String {
        lm.withLock(uniqueOp.op.key) {
            return executor.run(uniqueOp)
        }
    }
}