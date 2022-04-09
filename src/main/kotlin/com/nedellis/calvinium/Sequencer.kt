package com.nedellis.calvinium

import java.util.UUID

class Sequencer(private val scheduler: Scheduler) {
    fun run(txn: Transaction): String? {
        val uniqueTxn = UniqueTransaction(UUID.randomUUID(), txn)
        return scheduler.run(uniqueTxn)
    }
}