package com.nedellis.calvinium

import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.pathString
import org.rocksdb.Options
import org.rocksdb.RocksDB

interface ExecutorServer {
    fun setPartitionValue(txnUUID: UUID, recordKey: RecordKey, recordValue: String?)
    suspend fun getPartitionValue(txnUUID: UUID, recordKey: RecordKey): String?
}

class LocalExecutorServer() : ExecutorServer {
    private lateinit var allReplicas: Map<UUID, LocalExecutorServer>
    private val valueCache = ConcurrentHashMap<Pair<UUID, RecordKey>, String?>()

    override fun setPartitionValue(txnUUID: UUID, recordKey: RecordKey, recordValue: String?) {
        valueCache[Pair(txnUUID, recordKey)] = recordValue
    }

    override suspend fun getPartitionValue(txnUUID: UUID, recordKey: RecordKey): String? {
        val responsibleReplica = partitionForRecordKey(recordKey)
        return allReplicas[responsibleReplica]!!.getPartitionValueRPC(txnUUID, recordKey)
    }

    fun setAllReplicas(replicas: Map<UUID, LocalExecutorServer>) {
        allReplicas = replicas
    }

    private fun partitionForRecordKey(recordKey: RecordKey): UUID {
        val virtualNodeId = recordKey.virtualNodeId()
        val replicaIdx = virtualNodeId % allReplicas.size
        return allReplicas.keys.sorted()[replicaIdx]
    }

    /** @throws NoSuchElementException if the value does not yet exist in the map */
    private fun getPartitionValueRPC(uuid: UUID, recordKey: RecordKey): String? {
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

    fun run(uniqueTxn: UniqueTransaction): String? {
        val recordCache =
            uniqueTxn
                .txn
                .operations
                .map { it.key }
                .associateWith { db.get(it.toBytes())?.decodeToString() }
                .toMutableMap()

        for (op in uniqueTxn.txn.operations.dropLast(1)) {
            runOperation(op, recordCache)
        }

        val result = runOperation(uniqueTxn.txn.operations.last(), recordCache)

        // Write back record cache
        for (record in recordCache) {
            if (record.value != null) {
                db.put(record.key.toBytes(), record.value!!.encodeToByteArray())
            } else {
                db.delete(record.key.toBytes())
            }
        }

        return result
    }

    private fun runOperation(op: Operation, recordCache: MutableMap<RecordKey, String?>): String? {
        return when (op.type) {
            is Put -> {
                recordCache[op.key] = op.type.value
                op.type.value
            }
            Get -> {
                recordCache[op.key]
            }
            Delete -> {
                val oldValue = recordCache[op.key]
                recordCache[op.key] = null
                oldValue
            }
        }
    }
}
