package com.nedellis.calvinium.raft

import com.google.common.collect.ImmutableList
import com.tinder.StateMachine
import java.util.UUID
import kotlin.math.max
import kotlin.math.min
import mu.KotlinLogging

data class LogEntry(val term: Int, val command: Any)

data class State(
    val id: UUID,
    val currentTerm: Int = 0,
    val votedFor: UUID? = null,
    val log: ImmutableList<LogEntry> = ImmutableList.of(),
    val commitIndex: Int = 0,
    val lastApplied: Int = 0,
) {
    fun startElection(): State {
        return this.copy(currentTerm = currentTerm + 1, votedFor = id)
    }
    fun updateToLatestTerm(otherTerm: Int): State {
        return this.copy(currentTerm = max(currentTerm, otherTerm), votedFor = null)
    }
    fun updateLastApplied(leaderCommitIndex: Int): State {
        // TODO: Apply log[lastApplied] to State Machine if leaderCommitIndex > lastApplied
        return this.copy(lastApplied = max(lastApplied, leaderCommitIndex))
    }

    /** Returns if the operation will result in success */
    fun validateAppendEntries(rpc: RaftEvent.AppendEntriesRPC): Boolean {
        if (rpc.leaderTerm < currentTerm) {
            return false
        }
        if (log[rpc.prevLogIndex].term != rpc.prevLogTerm) {
            return false
        }
        return true
    }

    fun appendEntries(rpc: RaftEvent.AppendEntriesRPC): State {
        assert(validateAppendEntries(rpc))

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

        return this.copy(log = newLog, commitIndex = newCommitIndex)
    }

    private fun findFirstConflictingEntryIndex(
        prevLogIndex: Int,
        entries: ImmutableList<LogEntry>
    ): Int? {
        for (i in 0 until entries.size) {
            if (log[i + prevLogIndex + 1].term != entries[i].term) {
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
    val nextIndex: Map<UUID, Int> = mapOf(),
    val matchIndex: Map<UUID, Int> = mapOf()
)

sealed interface RaftState {
    val state: State
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
    }

    data class Leader(override val state: State, val leaderState: LeaderState) : RaftState {
        override fun withState(newState: State): RaftState {
            return this.copy(state = newState)
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

    fun appendEntriesAndUpdateToLatestTerm(
        leaderTerm: Int,
        leaderCommitIndex: Int
    ): Pair<RaftState, RaftSideEffect.AppendEntriesRPCResponse> {
        val updatedState =
            this.state.updateToLatestTerm(leaderTerm).updateLastApplied(leaderCommitIndex)
        return Pair(
            Follower(updatedState),
            RaftSideEffect.AppendEntriesRPCResponse(updatedState.currentTerm, true)
        )
    }

    fun convertToFollower(latestTerm: Int): RaftState {
        return Follower(this.state.updateToLatestTerm(latestTerm))
    }
}

sealed interface RaftEvent {
    object FollowerTimeOut : RaftEvent
    object CandidateElectionTimeOut : RaftEvent
    object CandidateMajorityVotesReceived : RaftEvent
    data class AppendEntriesRPC(
        val leaderTerm: Int,
        val leaderId: UUID,
        val prevLogIndex: Int,
        val prevLogTerm: Int,
        val entries: ImmutableList<LogEntry> = ImmutableList.of(),
        val leaderCommitIndex: Int
    ) : RaftEvent
    data class RequestVoteRPC(
        val candidateTerm: Int,
        val candidateId: UUID,
        val lastLogIndex: Int,
        val lastLogTerm: Int
    ) : RaftEvent
    data class AppendEntriesRPCResponse(val r: RaftSideEffect.AppendEntriesRPCResponse) : RaftEvent
    data class RequestVoteRPCResponse(val r: RaftSideEffect.RequestVoteRPCResponse) : RaftEvent
}

sealed class RaftSideEffect {
    data class StartRequestVoteRPCRequest(val currentState: State) : RaftSideEffect()
    data class StartAppendEntriesRPCRequest(val currentState: State) : RaftSideEffect()
    data class AppendEntriesRPCResponse(val clientTerm: Int, val success: Boolean) :
        RaftSideEffect()
    data class RequestVoteRPCResponse(val clientTerm: Int, val voteGranted: Boolean) :
        RaftSideEffect()
}

fun buildRaftStateMachine(id: UUID): StateMachine<RaftState, RaftEvent, RaftSideEffect> {
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
                    val (updatedState, sideEffect) =
                        this.appendEntriesAndUpdateToLatestTerm(it.leaderTerm, it.leaderCommitIndex)
                    transitionTo(updatedState, sideEffect)
                } else {
                    transitionTo(
                        RaftState.Follower(this.state.updateLastApplied(it.leaderCommitIndex)),
                        RaftSideEffect.AppendEntriesRPCResponse(this.state.currentTerm, true)
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
                        RaftState.Follower(updatedState),
                        RaftSideEffect.RequestVoteRPCResponse(
                            updatedState.currentTerm,
                            updatedState.wasVoteGranted(it.candidateId, it.candidateTerm)
                        )
                    )
                }
            }

            on<RaftEvent.FollowerTimeOut> {
                transitionTo(
                    RaftState.Candidate(this.state.startElection()),
                    RaftSideEffect.StartRequestVoteRPCRequest(this.state.startElection())
                )
            }
        }

        state<RaftState.Candidate> {
            on<RaftEvent.AppendEntriesRPC> {
                if (it.leaderTerm >= this.state.currentTerm) {
                    val (updatedState, sideEffect) =
                        this.appendEntriesAndUpdateToLatestTerm(it.leaderTerm, it.leaderCommitIndex)
                    transitionTo(updatedState, sideEffect)
                } else {
                    transitionTo(
                        RaftState.Candidate(this.state.updateLastApplied(it.leaderCommitIndex))
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
                transitionTo(
                    RaftState.Candidate(this.state.startElection()),
                    RaftSideEffect.StartRequestVoteRPCRequest(this.state.startElection())
                )
            }

            on<RaftEvent.CandidateMajorityVotesReceived> {
                transitionTo(
                    RaftState.Leader(this.state, LeaderState()),
                    RaftSideEffect.StartAppendEntriesRPCRequest(this.state)
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
                    val (updatedState, sideEffect) =
                        this.appendEntriesAndUpdateToLatestTerm(it.leaderTerm, it.leaderCommitIndex)
                    transitionTo(updatedState, sideEffect)
                } else {
                    transitionTo(RaftState.Leader(this.state, this.leaderState))
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
                        RaftState.Leader(updatedState, this.leaderState),
                        RaftSideEffect.RequestVoteRPCResponse(
                            updatedState.currentTerm,
                            updatedState.wasVoteGranted(it.candidateId, it.candidateTerm)
                        )
                    )
                }
            }

            on<RaftEvent.AppendEntriesRPCResponse> {
                if (it.r.clientTerm > this.state.currentTerm) {
                    transitionTo(this.convertToFollower(it.r.clientTerm))
                } else {
                    // TODO: Update last commit stuff
                    transitionTo(this)
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
