package com.nedellis.calvinium

import java.util.UUID

data class Transaction(val operations: List<Operation>)
data class Operation(val key: String, val type: OperationType)

sealed interface OperationType
data class Put(val value: String) : OperationType
object Get : OperationType
object Delete : OperationType

data class UniqueTransaction(val id: UUID, val txn: Transaction)