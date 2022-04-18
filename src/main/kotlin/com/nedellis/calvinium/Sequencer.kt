package com.nedellis.calvinium

import com.google.common.util.concurrent.AbstractExecutionThreadService
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import mu.KotlinLogging

private data class QueuedTxn(
    val uniqueTransaction: UniqueTransaction,
    val responseChannel: SendChannel<RecordValue>? = null
)

class LocalSequencerService(private val scheduler: Scheduler) : AbstractExecutionThreadService() {
    private val logger = KotlinLogging.logger {}
    private val workChannel = Channel<QueuedTxn>()
    private lateinit var otherSequencers: List<LocalSequencerService>

    override fun run() {
        runBlocking {
            while (isRunning) {
                withTimeoutOrNull(1000L) {
                    val txn = workChannel.receive()
                    val result = scheduler.run(txn.uniqueTransaction)
                    if (txn.responseChannel != null) {
                        txn.responseChannel.send(result)
                    }
                }
            }
        }
    }

    fun setAllSequencers(sequencers: List<LocalSequencerService>) {
        otherSequencers = sequencers.filter { s -> s != this }
    }

    private suspend fun executeTxnRPC(txn: UniqueTransaction) {
        workChannel.send(QueuedTxn(txn))
    }

    suspend fun executeTxn(txn: Transaction): RecordValue {
        val uniqueTxn = UniqueTransaction(UUID.randomUUID(), txn)
        val resultsChannel = Channel<RecordValue>()

        logger.debug { "Initiating TXN: $txn..." }
        logger.debug { "Sequencers: $otherSequencers" }

        withContext(Dispatchers.Default) {
            for (seq in otherSequencers) {
                launch { seq.executeTxnRPC(uniqueTxn) }
            }
            launch { workChannel.send(QueuedTxn(uniqueTxn, resultsChannel)) }
        }

        logger.debug { "Waiting on TXN: $txn..." }

        return resultsChannel.receive()
    }
}
