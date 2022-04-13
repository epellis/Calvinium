package com.nedellis.calvinium

import com.google.common.collect.ImmutableMap
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.io.path.pathString
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.update
import org.rocksdb.Options
import org.rocksdb.RocksDB

interface ExecutorServer {
    fun isRecordLocal(recordKey: RecordKey): Boolean
    fun allPartitions(): Set<UUID>
    fun allOtherPartitions(): Set<UUID>
    fun broadcastRecordCache(
        txnUUID: UUID,
        localRecordCache: Map<RecordKey, RecordValue>
    ): StateFlow<ImmutableMap<UUID, Map<RecordKey, RecordValue>>>
}

class LocalExecutorServer(val partitionUUID: UUID) : ExecutorServer {
    private lateinit var allPartitions: Map<UUID, LocalExecutorServer>

    private val lock = ReentrantLock()
    private val recordCacheFlowByTxnUUID =
        mutableMapOf<UUID, MutableStateFlow<ImmutableMap<UUID, Map<RecordKey, RecordValue>>>>()

    fun initAllPartitions(partitions: Map<UUID, LocalExecutorServer>) {
        allPartitions = partitions
    }

    private fun partitionForRecordKey(recordKey: RecordKey): UUID {
        val virtualNodeId = recordKey.virtualNodeId()
        val partitionIdx = virtualNodeId % allPartitions.size
        return allPartitions.keys.sorted()[partitionIdx]
    }

    override fun isRecordLocal(recordKey: RecordKey): Boolean {
        return partitionForRecordKey(recordKey) == partitionUUID
    }

    override fun allPartitions(): Set<UUID> {
        return allPartitions.keys
    }

    override fun allOtherPartitions(): Set<UUID> {
        return allPartitions.keys.filter { it != partitionUUID }.toSet()
    }

    override fun broadcastRecordCache(
        txnUUID: UUID,
        localRecordCache: Map<RecordKey, RecordValue>
    ): StateFlow<ImmutableMap<UUID, Map<RecordKey, RecordValue>>> {
        for (otherPartition in allPartitions.filterNot { it.key != partitionUUID }.values) {
            otherPartition.receiveRecordCache(txnUUID, partitionUUID, localRecordCache)
        }

        return lock.withLock {
            recordCacheFlowByTxnUUID
                .getOrPut(txnUUID) { MutableStateFlow(ImmutableMap.of()) }
                .asStateFlow()
        }
    }

    private fun receiveRecordCache(
        txnUUID: UUID,
        externalPartitionUUID: UUID,
        externalRecordCache: Map<RecordKey, RecordValue>
    ) {
        lock.withLock {
            val stateFlow =
                recordCacheFlowByTxnUUID.getOrPut(txnUUID) { MutableStateFlow(ImmutableMap.of()) }
            stateFlow.update { recordCache ->
                ImmutableMap.builder<UUID, Map<RecordKey, RecordValue>>()
                    .putAll(recordCache)
                    .putAll(mapOf(externalPartitionUUID to externalRecordCache))
                    .build()
            }
        }
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
        val localRecordCache = buildLocalRecordCache(uniqueTxn)

        val recordCacheFlow = executorServer.broadcastRecordCache(uniqueTxn.id, localRecordCache)

        val recordCache = buildRecordCache(recordCacheFlow).toMutableMap()

        // Return the result of the last operation
        for (op in uniqueTxn.txn.operations.dropLast(1)) {
            runOperation(op, recordCache)
        }
        val result = runOperation(uniqueTxn.txn.operations.last(), recordCache)

        flushRecordCache(uniqueTxn.id, recordCache)

        return result
    }

    private fun buildLocalRecordCache(uniqueTxn: UniqueTransaction): Map<RecordKey, RecordValue> {
        val localRecordKeys =
            uniqueTxn.txn.operations.map { it.key }.filter { executorServer.isRecordLocal(it) }
        return localRecordKeys.associateWith { RecordValue(db.get(it.toBytes())?.decodeToString()) }
    }

    private suspend fun buildRecordCache(
        recordCacheFlow: StateFlow<ImmutableMap<UUID, Map<RecordKey, RecordValue>>>
    ): Map<RecordKey, RecordValue> {
        val allPartitionUUIDs = executorServer.allOtherPartitions()
        return recordCacheFlow.takeIf { recordCache ->
                recordCache.value.keys == allPartitionUUIDs
            }!!
            .map { immutableRecordCache ->
                immutableRecordCache.toMap().values.flatMap { it.entries }.associate {
                    it.key to it.value
                }
            }
            .first()
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