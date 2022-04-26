package com.nedellis.calvinium.raft

import com.google.common.util.concurrent.AbstractExecutionThreadService
import com.nedellis.calvinium.proto.RaftGrpcKt

class RaftController : AbstractExecutionThreadService() {
    override fun run() {
        TODO("Not yet implemented")
    }
}

class RaftServer : RaftGrpcKt.RaftCoroutineImplBase() {}
