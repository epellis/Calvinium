package com.nedellis.calvinium.raft

import com.google.common.util.concurrent.AbstractExecutionThreadService
import com.nedellis.calvinium.proto.AppendEntriesRequest
import com.nedellis.calvinium.proto.AppendEntriesResponse
import com.nedellis.calvinium.proto.RaftGrpcKt
import java.util.UUID

class RaftController(id: UUID) : AbstractExecutionThreadService() {
    private val raftState = buildRaftStateMachine(id)

    override fun run() {
        TODO("Not yet implemented")
    }
}

class RaftServer : RaftGrpcKt.RaftCoroutineImplBase() {
    override suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
        return super.appendEntries(request)
    }
}
