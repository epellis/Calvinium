package com.nedellis.calvinium

import com.google.common.util.concurrent.AbstractExecutionThreadService
import java.util.UUID
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit

private data class QueuedTxn(
    val uniqueTransaction: UniqueTransaction,
    val shouldReturnResults: Boolean
)

class LocalSequencerService(private val scheduler: Scheduler) : AbstractExecutionThreadService() {
    private val workQueue = SynchronousQueue<QueuedTxn>()
    private val resultsQueue = SynchronousQueue<RecordValue>()
    private lateinit var otherSequencers: List<LocalSequencerService>

    override fun run() {
        while (isRunning) {
            val txn = workQueue.poll(1, TimeUnit.SECONDS)
            if (txn != null) {
                val result = scheduler.run(txn.uniqueTransaction)
                if (txn.shouldReturnResults) {
                    resultsQueue.put(result)
                }
            }
        }
    }

    fun setOtherSequencers(sequencers: List<LocalSequencerService>) {
        otherSequencers = sequencers
    }

    private fun executeTxnRPC(txn: UniqueTransaction) {
        workQueue.put(QueuedTxn(txn, false))
    }

    fun executeTxn(txn: Transaction): RecordValue {
        val uniqueTxn = UniqueTransaction(UUID.randomUUID(), txn)
        for (seq in otherSequencers) {
            seq.executeTxnRPC(uniqueTxn)
        }
        workQueue.put(QueuedTxn(uniqueTxn, true))
        return resultsQueue.take()
    }
}