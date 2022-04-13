package com.nedellis.calvinium

import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.pathString
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.rocksdb.Options
import org.rocksdb.RocksDB

interface ExecutorServer {
    fun isRecordLocal(recordKey: RecordKey): Boolean
    suspend fun setPartitionValue(txnUUID: UUID, recordKey: RecordKey, recordValue: RecordValue)
    suspend fun getPartitionValue(txnUUID: UUID, recordKey: RecordKey): RecordValue
}

class LocalExecutorServer(val partitionUUID: UUID) : ExecutorServer {
    private lateinit var allPartitions: Map<UUID, LocalExecutorServer>
    private val valueCache = ConcurrentHashMap<Pair<UUID, RecordKey>, RecordValue>()

    private val valueCacheWaiters = mutableMapOf<Pair<UUID, RecordKey>, SendChannel<RecordValue>>()
    private val valueCacheWaitersLock = Mutex()

    override suspend fun setPartitionValue(
        txnUUID: UUID,
        recordKey: RecordKey,
        recordValue: RecordValue
    ) {
        valueCacheWaitersLock.withLock {
            valueCache[Pair(txnUUID, recordKey)] = recordValue
            valueCacheWaiters.remove(Pair(txnUUID, recordKey))?.send(recordValue)
        }
    }

    override fun isRecordLocal(recordKey: RecordKey): Boolean {
        return partitionForRecordKey(recordKey) == partitionUUID
    }

    override suspend fun getPartitionValue(txnUUID: UUID, recordKey: RecordKey): RecordValue {
        val responsiblePartition = partitionForRecordKey(recordKey)
        return allPartitions[responsiblePartition]!!.getPartitionValueRPC(txnUUID, recordKey)
    }

    fun setAllPartitions(partitions: Map<UUID, LocalExecutorServer>) {
        allPartitions = partitions
    }

    private fun partitionForRecordKey(recordKey: RecordKey): UUID {
        val virtualNodeId = recordKey.virtualNodeId()
        val partitionIdx = virtualNodeId % allPartitions.size
        return allPartitions.keys.sorted()[partitionIdx]
    }

    private suspend fun getPartitionValueRPC(txnUUID: UUID, recordKey: RecordKey): RecordValue {
        val responseChannel = Channel<RecordValue>(Channel.BUFFERED)

        valueCacheWaitersLock.withLock {
            val cachedRecordValue = valueCache[Pair(txnUUID, recordKey)]
            if (cachedRecordValue != null) {
                responseChannel.send(cachedRecordValue)
            } else {
                valueCacheWaiters[Pair(txnUUID, recordKey)] = responseChannel
            }
        }

        return responseChannel.receive()
    }
}

class Executor(private val executorServer: ExecutorServer) {
    init {
        RocksDB.loadLibrary()
    }

    private val rocksDirectory = Files.createTempDirectory("calvinium")
    private val options = Options()

    init {
        options.setCreateIfMissing(true)
    }

    private val db = RocksDB.open(options, rocksDirectory.pathString)

    suspend fun run(uniqueTxn: UniqueTransaction): RecordValue {
        val recordCache = buildRecordCache(uniqueTxn).toMutableMap()

        // Return the result of the last operation
        for (op in uniqueTxn.txn.operations.dropLast(1)) {
            runOperation(op, recordCache)
        }
        val result = runOperation(uniqueTxn.txn.operations.last(), recordCache)

        flushRecordCache(uniqueTxn.id, recordCache)

        return result
    }

    private suspend fun buildRecordCache(
        uniqueTxn: UniqueTransaction
    ): Map<RecordKey, RecordValue> {
        val localRecordKeys =
            uniqueTxn.txn.operations.map { it.key }.filter { executorServer.isRecordLocal(it) }
        val localRecordCache =
            localRecordKeys.associateWith { RecordValue(db.get(it.toBytes())?.decodeToString()) }

        for (localRecord in localRecordCache) {
            executorServer.setPartitionValue(uniqueTxn.id, localRecord.key, localRecord.value)
        }

        val externalRecordKeys =
            uniqueTxn.txn.operations.map { it.key }.filterNot { executorServer.isRecordLocal(it) }
        val externalRecordCache =
            externalRecordKeys.associateWith { executorServer.getPartitionValue(uniqueTxn.id, it) }

        return localRecordCache.plus(externalRecordCache)
    }

    private fun flushRecordCache(txnUUID: UUID, recordCache: Map<RecordKey, RecordValue>) {
        for (record in recordCache) {
            if (executorServer.isRecordLocal(record.key)) {
                if (record.value.contents != null) {
                    db.put(record.key.toBytes(), record.value.contents!!.encodeToByteArray())
                } else {
                    db.delete(record.key.toBytes())
                }
            }
        }
    }

    private fun runOperation(
        op: Operation,
        recordCache: MutableMap<RecordKey, RecordValue>
    ): RecordValue {
        return when (op.type) {
            is Put -> {
                recordCache[op.key] = RecordValue(op.type.value)
                RecordValue(op.type.value)
            }
            Get -> {
                recordCache[op.key]!!
            }
            Delete -> {
                val oldValue = recordCache[op.key]!!
                recordCache[op.key] = RecordValue()
                oldValue
            }
        }
    }
}