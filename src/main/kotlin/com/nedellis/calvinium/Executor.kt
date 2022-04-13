package com.nedellis.calvinium

import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedRunnable
import java.nio.file.Files
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.io.path.pathString
import org.rocksdb.Options
import org.rocksdb.RocksDB

interface ExecutorServer {
    fun setPartitionValue(txnUUID: UUID, recordKey: RecordKey, recordValue: RecordValue)
    fun isRecordLocal(recordKey: RecordKey): Boolean
    fun getPartitionValue(txnUUID: UUID, recordKey: RecordKey): RecordValue
}

class LocalExecutorServer(val partitionUUID: UUID) : ExecutorServer {
    private lateinit var allPartitions: Map<UUID, LocalExecutorServer>
    private val valueCache = ConcurrentHashMap<Pair<UUID, RecordKey>, RecordValue>()
    private val valueCacheLock = ReentrantLock()

    override fun setPartitionValue(txnUUID: UUID, recordKey: RecordKey, recordValue: RecordValue) {
        valueCache[Pair(txnUUID, recordKey)] = recordValue
    }

    override fun isRecordLocal(recordKey: RecordKey): Boolean {
        return partitionForRecordKey(recordKey) == partitionUUID
    }

    override fun getPartitionValue(txnUUID: UUID, recordKey: RecordKey): RecordValue {
        val responsiblePartition = partitionForRecordKey(recordKey)

        val retryPolicy =
            RetryPolicy.builder<Any>()
                .handle(NoSuchElementException::class.java)
                .withDelay(Duration.ofSeconds(1))
                .withMaxRetries(10)
                .build()

        var partitionValue = RecordValue()

        Failsafe.with(retryPolicy)
            .run(
                CheckedRunnable {
                    partitionValue =
                        allPartitions[responsiblePartition]!!.getPartitionValueRPC(
                            txnUUID, recordKey)
                })

        return partitionValue
    }

    fun setAllPartitions(partitions: Map<UUID, LocalExecutorServer>) {
        allPartitions = partitions
    }

    private fun partitionForRecordKey(recordKey: RecordKey): UUID {
        val virtualNodeId = recordKey.virtualNodeId()
        val partitionIdx = virtualNodeId % allPartitions.size
        return allPartitions.keys.sorted()[partitionIdx]
    }

    /** @throws NoSuchElementException if the value does not yet exist in the map */
    @Synchronized
    private fun getPartitionValueRPC(uuid: UUID, recordKey: RecordKey): RecordValue {
        return valueCache.getValue(Pair(uuid, recordKey))
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

    fun run(uniqueTxn: UniqueTransaction): RecordValue {
        val recordCache = buildRecordCache(uniqueTxn).toMutableMap()

        // Return the result of the last operation
        for (op in uniqueTxn.txn.operations.dropLast(1)) {
            runOperation(op, recordCache)
        }
        val result = runOperation(uniqueTxn.txn.operations.last(), recordCache)

        flushRecordCache(uniqueTxn.id, recordCache)

        return result
    }

    private fun buildRecordCache(uniqueTxn: UniqueTransaction): Map<RecordKey, RecordValue> {
        val recordCache =
            uniqueTxn
                .txn
                .operations
                .map { it.key }
                .associateWith { RecordValue(db.get(it.toBytes())?.decodeToString()) }
                .toMutableMap()

        val localRecordKeys =
            uniqueTxn.txn.operations.map { it.key }.filter { executorServer.isRecordLocal(it) }
        val localRecordCache =
            localRecordKeys.associateWith { db.get(it.toBytes())?.decodeToString() }

        for (record in recordCache) {
            executorServer.setPartitionValue(uniqueTxn.id, record.key, record.value)
        }

        val externalRecordKeys =
            uniqueTxn.txn.operations.map { it.key }.filterNot { executorServer.isRecordLocal(it) }
        val externalRecordCache =
            externalRecordKeys.associateWith { executorServer.getPartitionValue(uniqueTxn.id, it) }

        return recordCache
    }

    private fun flushRecordCache(txnUUID: UUID, recordCache: Map<RecordKey, RecordValue>) {
        for (record in recordCache) {
            if (record.value.contents != null) {
                db.put(record.key.toBytes(), record.value.contents!!.encodeToByteArray())
            } else {
                db.delete(record.key.toBytes())
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