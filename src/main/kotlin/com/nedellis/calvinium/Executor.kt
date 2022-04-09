package com.nedellis.calvinium

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

    fun run(uniqueOp: UniqueOperation): String {
        return when (uniqueOp.op.type) {
            is Put -> put(uniqueOp, uniqueOp.op.type.value)
            Get -> get(uniqueOp)
            Delete -> delete(uniqueOp)
        }
    }

    private fun put(uniqueOp: UniqueOperation, value: String): String {
        db.put(uniqueOp.op.key.encodeToByteArray(), value.encodeToByteArray())
        return value
    }

    private fun get(uniqueOp: UniqueOperation): String {
        return db.get(uniqueOp.op.key.encodeToByteArray()).decodeToString()
    }

    private fun delete(uniqueOp: UniqueOperation): String {
        val previousValue = db.get(uniqueOp.op.key.encodeToByteArray()).decodeToString()
        db.delete(uniqueOp.op.key.encodeToByteArray())
        return previousValue
    }
}