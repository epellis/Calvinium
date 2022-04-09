package com.nedellis.calvinium

import java.util.UUID

data class Operation(val key: String, val type: OperationType)

sealed interface OperationType
data class Put(val value: String) : OperationType
object Get : OperationType
object Delete : OperationType

data class UniqueOperation(val id: UUID, val op: Operation)