package com.nedellis.calvinium

import java.util.UUID

class Sequencer(private val scheduler: Scheduler) {
    fun run(op: Operation): String {
        val uniqueOp = UniqueOperation(UUID.randomUUID(), op)
        return scheduler.run(uniqueOp)
    }
}