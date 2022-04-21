package com.nedellis.calvinium.raft

import com.tinder.StateMachine
import java.util.UUID
import kotlin.math.max

data class State(
    val id: UUID,
    val currentTerm: Int = 0,
    val votedFor: UUID? = null,
    val log: List<Any> = listOf(),
    val commitIndex: Int = 0,
    val lastApplied: Int = 0,
) {
    fun startElection(): State {
        return this.copy(currentTerm = currentTerm + 1, votedFor = id)
    }
    fun updateToLatestTerm(otherTerm: Int): State {
        return this.copy(currentTerm = max(currentTerm, otherTerm), votedFor = null)
    }
}

data class LeaderState(
    val nextIndex: Map<UUID, Int> = mapOf(),
    val matchIndex: Map<UUID, Int> = mapOf()
)

sealed class RaftState {
    data class Follower(val state: State) : RaftState()
    data class Candidate(val state: State) : RaftState()
    data class Leader(val state: State, val leaderState: LeaderState) : RaftState()
}

sealed class RaftEvent {
    object FollowerTimeOut : RaftEvent()
    object CandidateElectionTimeOut : RaftEvent()
    object CandidateMajorityVotesReceived : RaftEvent()
    data class CandidateNewTerm(val newTerm: Int) : RaftEvent()
    data class LeaderDiscoversHigherTerm(val newTerm: Int) : RaftEvent()
    data class AppendEntriesRPC(
        val leaderTerm: Int,
        val leaderId: UUID,
        val prevLogIndex: Int,
        val entries: List<Any>,
        val leaderCommitIndex: Int
    ) : RaftEvent()
    data class RequestVoteRPC(
        val candidateTerm: Int,
        val candidateId: UUID,
        val lastLogIndex: Int,
        val lastLogTerm: Int
    )
}

sealed class RaftSideEffect {
    data class StartElectionResponse(val currentState: State) : RaftSideEffect()
    data class AppendEntriesResponse(val currentState: State) : RaftSideEffect()
    data class RequestVoteResponse(val currentState: State) : RaftSideEffect()
}

fun buildRaftStateMachine(id: UUID): StateMachine<RaftState, RaftEvent, RaftSideEffect> {
    return buildArbitraryRaftStateMachine(RaftState.Follower(State(id)))
}

internal fun buildArbitraryRaftStateMachine(
    initialState: RaftState
): StateMachine<RaftState, RaftEvent, RaftSideEffect> {
    return StateMachine.create {
        initialState(initialState)

        state<RaftState.Follower> {
            on<RaftEvent.FollowerTimeOut> {
                transitionTo(
                    RaftState.Candidate(this.state.startElection()),
                    RaftSideEffect.StartElectionResponse(this.state.startElection()))
            }
        }

        state<RaftState.Candidate> {
            on<RaftEvent.CandidateElectionTimeOut> {
                transitionTo(
                    RaftState.Candidate(this.state.startElection()),
                    RaftSideEffect.StartElectionResponse(this.state.startElection()))
            }
            on<RaftEvent.CandidateMajorityVotesReceived> {
                transitionTo(
                    RaftState.Leader(this.state, LeaderState()),
                    RaftSideEffect.AppendEntriesResponse(this.state))
            }
            on<RaftEvent.CandidateNewTerm> {
                transitionTo(RaftState.Follower(this.state.updateToLatestTerm(it.newTerm)))
            }
        }

        state<RaftState.Leader> {
            on<RaftEvent.LeaderDiscoversHigherTerm> { transitionTo(RaftState.Follower(this.state)) }
        }

        onTransition {
            val validTransition = it as? StateMachine.Transition.Valid ?: return@onTransition
            when (validTransition.sideEffect) {
                else -> {}
            }
        }
    }
}
