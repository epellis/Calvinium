package com.nedellis.calvinium

import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.Options
import org.rocksdb.RocksDB
import java.nio.file.Files
import kotlin.io.path.pathString

class Executor {
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
            uniqueTxn.txn.operations.map { it.key }.associateWith { db.get(it.toBytes())?.decodeToString() }
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