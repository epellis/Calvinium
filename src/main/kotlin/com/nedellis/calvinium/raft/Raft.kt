package com.nedellis.calvinium.raft

import com.google.common.collect.ImmutableList
import com.tinder.StateMachine
import java.util.UUID
import kotlin.math.max
import kotlin.math.min
import mu.KotlinLogging

data class LogEntry(val term: Int, val command: Any? = null)

internal data class State(
    val id: UUID,
    val currentTerm: Int = 0,
    val votedFor: UUID? = null,
    val log: ImmutableList<LogEntry> = ImmutableList.of(LogEntry(term = 0)),
    val commitIndex: Int = 0,
    val lastApplied: Int = 0,
) {
    init {
        assert(lastApplied <= commitIndex)
    }

    private val logger = KotlinLogging.logger {}

    fun startElection(): Pair<State, RaftEvent.RequestVoteRPC> {
        val updatedState = this.copy(currentTerm = currentTerm + 1, votedFor = id)
        return Pair(
            updatedState,
            RaftEvent.RequestVoteRPC(
                updatedState.currentTerm,
                updatedState.id,
                updatedState.log.last().term
            )
        )
    }

    fun updateToLatestTerm(otherTerm: Int): State {
        return this.copy(currentTerm = max(currentTerm, otherTerm), votedFor = null)
    }

    fun isAppendEntriesValid(rpc: RaftEvent.AppendEntriesRPC): Boolean {
        if (rpc.leaderTerm < currentTerm) {
            logger.debug {
                "Append Entries $rpc invalid because Current Term $currentTerm > Leader Term ${rpc.leaderTerm}"
            }
            return false
        }

        val entryAtPrevLogIndex = log.getOrNull(rpc.prevLogIndex)
        if (entryAtPrevLogIndex == null) {
            logger.debug {
                "Append Entries $rpc invalid because my log has no entry at ${rpc.prevLogIndex}"
            }
            return false
        } else if (entryAtPrevLogIndex.term != rpc.prevLogTerm) {
            logger.debug {
                "Append Entries $rpc invalid because my log term=${entryAtPrevLogIndex.term} != leader log term=${rpc.prevLogTerm}"
            }
            return false
        }

        return true
    }

    fun appendEntries(rpc: RaftEvent.AppendEntriesRPC): State {
        assert(isAppendEntriesValid(rpc))

        val listBuilder = ImmutableList.Builder<LogEntry>()
        listBuilder.addAll(log.subList(0, rpc.prevLogIndex + 1))

        val firstConflictingEntryIndex =
            findFirstConflictingEntryIndex(rpc.prevLogIndex, rpc.entries)

        val truncatedLog =
            if (firstConflictingEntryIndex == null) {
                log
            } else {
                log.subList(0, firstConflictingEntryIndex + rpc.prevLogIndex + 1)
            }

        val newLog =
            ImmutableList.Builder<LogEntry>()
                .addAll(truncatedLog)
                .addAll(rpc.entries.subList(firstConflictingEntryIndex ?: 0, rpc.entries.size))
                .build()

        val newCommitIndex =
            if (rpc.leaderCommitIndex > commitIndex) {
                min(rpc.leaderCommitIndex, newLog.size - 1)
            } else {
                commitIndex
            }

        // TODO: Update lastApplied

        return this.copy(log = newLog, commitIndex = newCommitIndex)
    }

    private fun findFirstConflictingEntryIndex(
        prevLogIndex: Int,
        entries: ImmutableList<LogEntry>
    ): Int? {
        for (i in 0 until entries.size) {
            if (i + prevLogIndex + 1 >= log.size ||
                    log[i + prevLogIndex + 1].term != entries[i].term
            ) {
                return i
            }
        }
        return null
    }

    fun wasVoteGranted(candidateId: UUID, candidateTerm: Int): Boolean {
        return (votedFor == candidateId && candidateTerm >= currentTerm)
    }

    fun tryGrantVote(candidateId: UUID, candidateTerm: Int): State {
        return if ((votedFor == null || votedFor == candidateId) && candidateTerm >= currentTerm) {
            this.copy(votedFor = candidateId)
        } else {
            this
        }
    }
}

data class LeaderState(
    // Index of the next entry the follower will send to each leader
    val nextIndex: Map<UUID, Int> = mapOf(),
    val matchIndex: Map<UUID, Int> = mapOf()
) {
    fun updateNextIndex(id: UUID, newNextIndex: Int): LeaderState {
        return this.copy(nextIndex = this.nextIndex.plus(Pair(id, newNextIndex)))
    }

    fun updateMatchIndex(id: UUID, newMatchIndex: Int): LeaderState {
        return this.copy(matchIndex = this.matchIndex.plus(Pair(id, newMatchIndex)))
    }
}

internal sealed interface RaftState {
    val state: State

    // TODO: Investigate getting rid of this
    fun withState(newState: State): RaftState

    data class Follower(override val state: State) : RaftState {
        override fun withState(newState: State): RaftState {
            return this.copy(state = newState)
        }
    }

    data class Candidate(override val state: State) : RaftState {
        override fun withState(newState: State): RaftState {
            return this.copy(state = newState)
        }

        fun convertToLeader(allServerIds: List<UUID>): Pair<Leader, RaftEvent.AppendEntriesRPC> {
            // initializes all nextIndex values to the index just after the
            // last one in its log
            val nextIndex = allServerIds.associateWith { 1 }

            //
            val matchIndex = allServerIds.associateWith { 0 }

            val leaderState = LeaderState(nextIndex, matchIndex)

            return Pair(
                Leader(state, leaderState),
                RaftEvent.AppendEntriesRPC(
                    leaderId = state.id,
                    leaderTerm = state.currentTerm,
                    prevLogIndex = state.log.size - 1,
                    prevLogTerm = state.log.last().term,
                    leaderCommitIndex = state.commitIndex,
                    entries = ImmutableList.of()
                )
            )
        }
    }

    data class Leader(override val state: State, val leaderState: LeaderState) : RaftState {
        override fun withState(newState: State): RaftState {
            return this.copy(state = newState)
        }

        fun updateIndicesOnSuccessfulAppendEntries(
            rpc: RaftEvent.AppendEntriesRPCResponse
        ): Leader {
            assert(rpc.res.success)

            val newNextIndex = this.leaderState.nextIndex[rpc.id]!! + rpc.req.entries.size

            val newMatchIndex = this.leaderState.matchIndex[rpc.id]!! + rpc.req.entries.size

            return this.copy(
                leaderState =
                    leaderState
                        .updateNextIndex(rpc.id, newNextIndex)
                        .updateMatchIndex(rpc.id, newMatchIndex)
            )
        }

        fun walkBackNextIndexAndRetry(
            rpc: RaftEvent.AppendEntriesRPCResponse
        ): Pair<Leader, RaftSideEffect.StartAppendEntriesRPCRequest> {
            assert(!rpc.res.success)

            val newNextIndex = this.leaderState.nextIndex[rpc.id]!! - 1

            val updatedState =
                this.copy(leaderState = leaderState.updateNextIndex(rpc.id, newNextIndex))

            return Pair(
                updatedState,
                RaftSideEffect.StartAppendEntriesRPCRequest(
                    mapOf(rpc.id to rpc.req.copy(prevLogIndex = newNextIndex - 1))
                )
            )
        }
    }

    fun requestVoteAndUpdateToLatestTerm(
        candidateId: UUID,
        candidateTerm: Int
    ): Pair<RaftState, RaftSideEffect.RequestVoteRPCResponse> {
        val updatedState =
            this.state.updateToLatestTerm(candidateTerm).tryGrantVote(candidateId, candidateTerm)
        return Pair(
            Follower(updatedState),
            RaftSideEffect.RequestVoteRPCResponse(
                updatedState.currentTerm,
                voteGranted = updatedState.wasVoteGranted(candidateId, candidateTerm)
            )
        )
    }

    fun appendEntriesAndConvertToFollower(
        rpc: RaftEvent.AppendEntriesRPC
    ): Pair<Follower, RaftSideEffect.AppendEntriesRPCResponse> {
        return if (this.state.isAppendEntriesValid(rpc)) {
            val updatedState = this.state.updateToLatestTerm(rpc.leaderTerm).appendEntries(rpc)
            Pair(
                Follower(updatedState),
                RaftSideEffect.AppendEntriesRPCResponse(updatedState.currentTerm, true)
            )
        } else {
            val updatedState = this.state.updateToLatestTerm(rpc.leaderTerm)
            Pair(
                Follower(updatedState),
                RaftSideEffect.AppendEntriesRPCResponse(updatedState.currentTerm, false)
            )
        }
    }

    fun convertToFollower(latestTerm: Int): RaftState {
        return Follower(this.state.updateToLatestTerm(latestTerm))
    }
}

internal sealed interface RaftEvent {
    data class FollowerTimeOut(val allServerIds: List<UUID>) : RaftEvent
    data class CandidateElectionTimeOut(val allServerIds: List<UUID>) : RaftEvent
    data class CandidateMajorityVotesReceived(val allServerIds: List<UUID>) : RaftEvent
    data class AppendEntriesRPC(
        val leaderTerm: Int,
        val leaderId: UUID,
        val prevLogIndex: Int = 0,
        val prevLogTerm: Int = 0,
        val leaderCommitIndex: Int = 0,
        val entries: ImmutableList<LogEntry> = ImmutableList.of(),
    ) : RaftEvent
    data class RequestVoteRPC(
        val candidateTerm: Int,
        val candidateId: UUID,
        val lastLogIndex: Int = 0,
        val lastLogTerm: Int = 0
    ) : RaftEvent
    data class AppendEntriesRPCResponse(
        val id: UUID,
        val req: AppendEntriesRPC,
        val res: RaftSideEffect.AppendEntriesRPCResponse
    ) : RaftEvent
    data class RequestVoteRPCResponse(val r: RaftSideEffect.RequestVoteRPCResponse) : RaftEvent
}

internal sealed interface RaftSideEffect {
    data class StartRequestVoteRPCRequest(val requests: Map<UUID, RaftEvent.RequestVoteRPC>) :
        RaftSideEffect
    data class StartAppendEntriesRPCRequest(val requests: Map<UUID, RaftEvent.AppendEntriesRPC>) :
        RaftSideEffect
    data class AppendEntriesRPCResponse(val clientTerm: Int, val success: Boolean) : RaftSideEffect
    data class RequestVoteRPCResponse(val clientTerm: Int, val voteGranted: Boolean) :
        RaftSideEffect
}

internal fun buildRaftStateMachine(id: UUID): StateMachine<RaftState, RaftEvent, RaftSideEffect> {
    return buildArbitraryRaftStateMachine(RaftState.Follower(State(id)))
}

internal fun buildArbitraryRaftStateMachine(
    initialState: RaftState
): StateMachine<RaftState, RaftEvent, RaftSideEffect> {
    val logger = KotlinLogging.logger {}

    return StateMachine.create {
        initialState(initialState)

        state<RaftState.Follower> {
            on<RaftEvent.AppendEntriesRPC> {
                if (it.leaderTerm > this.state.currentTerm) {
                    val (updatedState, sideEffect) = this.appendEntriesAndConvertToFollower(it)
                    transitionTo(updatedState, sideEffect)
                } else {
                    if (this.state.isAppendEntriesValid(it)) {
                        transitionTo(
                            this.withState(this.state.appendEntries(it)),
                            RaftSideEffect.AppendEntriesRPCResponse(this.state.currentTerm, true)
                        )
                    } else {
                        transitionTo(
                            this,
                            RaftSideEffect.AppendEntriesRPCResponse(this.state.currentTerm, false)
                        )
                    }
                }
            }

            on<RaftEvent.RequestVoteRPC> {
                if (it.candidateTerm > this.state.currentTerm) {
                    val (updatedState, sideEffect) =
                        this.requestVoteAndUpdateToLatestTerm(it.candidateId, it.candidateTerm)
                    transitionTo(updatedState, sideEffect)
                } else {
                    val updatedState = this.state.tryGrantVote(it.candidateId, it.candidateTerm)
                    transitionTo(
                        RaftState.Follower(updatedState),
                        RaftSideEffect.RequestVoteRPCResponse(
                            updatedState.currentTerm,
                            updatedState.wasVoteGranted(it.candidateId, it.candidateTerm)
                        )
                    )
                }
            }

            on<RaftEvent.FollowerTimeOut> {
                val (updatedState, rpc) = this.state.startElection()
                transitionTo(
                    RaftState.Candidate(updatedState),
                    RaftSideEffect.StartRequestVoteRPCRequest(it.allServerIds.associateWith { rpc })
                )
            }
        }

        state<RaftState.Candidate> {
            on<RaftEvent.AppendEntriesRPC> {
                if (it.leaderTerm >= this.state.currentTerm) {
                    val (updatedState, sideEffect) = this.appendEntriesAndConvertToFollower(it)
                    transitionTo(updatedState, sideEffect)
                } else {
                    transitionTo(
                        this,
                        RaftSideEffect.AppendEntriesRPCResponse(this.state.currentTerm, false)
                    )
                }
            }

            on<RaftEvent.RequestVoteRPC> {
                if (it.candidateTerm > this.state.currentTerm) {
                    val (updatedState, sideEffect) =
                        this.requestVoteAndUpdateToLatestTerm(it.candidateId, it.candidateTerm)
                    transitionTo(updatedState, sideEffect)
                } else {
                    val updatedState = this.state.tryGrantVote(it.candidateId, it.candidateTerm)
                    transitionTo(
                        RaftState.Candidate(updatedState),
                        RaftSideEffect.RequestVoteRPCResponse(
                            updatedState.currentTerm,
                            updatedState.wasVoteGranted(it.candidateId, it.candidateTerm)
                        )
                    )
                }
            }

            on<RaftEvent.CandidateElectionTimeOut> {
                val (updatedState, rpc) = this.state.startElection()
                transitionTo(
                    RaftState.Candidate(updatedState),
                    RaftSideEffect.StartRequestVoteRPCRequest(it.allServerIds.associateWith { rpc })
                )
            }

            on<RaftEvent.CandidateMajorityVotesReceived> {
                val (updatedState, rpc) = this.convertToLeader(it.allServerIds)
                transitionTo(
                    updatedState,
                    RaftSideEffect.StartAppendEntriesRPCRequest(
                        it.allServerIds.associateWith { rpc }
                    )
                )
            }

            on<RaftEvent.RequestVoteRPCResponse> {
                if (it.r.clientTerm > this.state.currentTerm) {
                    transitionTo(this.convertToFollower(it.r.clientTerm))
                } else {
                    transitionTo(this)
                }
            }
        }

        state<RaftState.Leader> {
            on<RaftEvent.AppendEntriesRPC> {
                if (it.leaderTerm >= this.state.currentTerm) {
                    val (updatedState, sideEffect) = this.appendEntriesAndConvertToFollower(it)
                    transitionTo(updatedState, sideEffect)
                } else {
                    transitionTo(
                        this,
                        RaftSideEffect.AppendEntriesRPCResponse(this.state.currentTerm, false)
                    )
                }
            }

            on<RaftEvent.RequestVoteRPC> {
                if (it.candidateTerm > this.state.currentTerm) {
                    val (updatedState, sideEffect) =
                        this.requestVoteAndUpdateToLatestTerm(it.candidateId, it.candidateTerm)
                    transitionTo(updatedState, sideEffect)
                } else {
                    val updatedState = this.state.tryGrantVote(it.candidateId, it.candidateTerm)
                    assert(!updatedState.wasVoteGranted(it.candidateId, it.candidateTerm)) {
                        "Leader should never vote for candidate of same term"
                    }
                    transitionTo(
                        RaftState.Leader(updatedState, this.leaderState),
                        RaftSideEffect.RequestVoteRPCResponse(
                            updatedState.currentTerm,
                            updatedState.wasVoteGranted(it.candidateId, it.candidateTerm)
                        )
                    )
                }
            }

            on<RaftEvent.AppendEntriesRPCResponse> {
                if (it.res.clientTerm > this.state.currentTerm) {
                    transitionTo(this.convertToFollower(it.res.clientTerm))
                } else {
                    if (it.res.success) {
                        transitionTo(this.updateIndicesOnSuccessfulAppendEntries(it))
                    } else {
                        logger.warn { "Append Entries failed: ${it.res}, my state: $this" }
                        val (updatedState, sideEffect) = this.walkBackNextIndexAndRetry(it)
                        transitionTo(updatedState, sideEffect)
                    }
                }
            }
        }

        onTransition {
            val validTransition = it as? StateMachine.Transition.Valid ?: return@onTransition
            when (validTransition.sideEffect) {
                else -> {}
            }
        }
    }
}
