package com.nedellis.calvinium.raft

import com.google.common.collect.ImmutableList
import com.tinder.StateMachine
import io.kotest.core.spec.style.FunSpec
import io.kotest.datatest.withData
import io.kotest.matchers.shouldBe
import java.util.UUID

private fun verifyTransition(
    startingState: RaftState,
    event: RaftEvent,
    endingState: RaftState,
    sideEffect: RaftSideEffect?
) {
    val stateMachine = buildArbitraryRaftStateMachine(startingState)
    val transition = stateMachine.transition(event) as StateMachine.Transition.Valid<*, *, *>
    stateMachine.state shouldBe endingState
    transition.sideEffect shouldBe sideEffect
}

private val THIS_RAFT_ID = UUID.nameUUIDFromBytes(byteArrayOf(0, 0))
private val OTHER_RAFT_ID = UUID.nameUUIDFromBytes(byteArrayOf(0, 1))

class RaftSuite :
    FunSpec({
        context("Always convert to follower when receiving append entries of higher term") {
            withData(
                listOf(
                    RaftState.Follower(
                        State(
                            THIS_RAFT_ID,
                            currentTerm = 1,
                        )
                    ),
                    RaftState.Candidate(
                        State(
                            THIS_RAFT_ID,
                            currentTerm = 1,
                        )
                    ),
                    RaftState.Leader(
                        State(
                            THIS_RAFT_ID,
                            currentTerm = 1,
                        ),
                        LeaderState()
                    ),
                )
            ) { raftState ->
                verifyTransition(
                    raftState,
                    RaftEvent.AppendEntriesRPC(
                        leaderTerm = 2,
                        leaderId = OTHER_RAFT_ID,
                        prevLogIndex = 0,
                        prevLogTerm = 0,
                        entries = ImmutableList.of(),
                        leaderCommitIndex = 0
                    ),
                    RaftState.Follower(State(THIS_RAFT_ID, currentTerm = 2)),
                    RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, true)
                )
            }
        }

        context("Always convert to follower when receiving request vote of higher term") {
            withData(
                listOf(
                    RaftState.Follower(
                        State(
                            THIS_RAFT_ID,
                            currentTerm = 1,
                        )
                    ),
                    RaftState.Candidate(
                        State(
                            THIS_RAFT_ID,
                            currentTerm = 1,
                        )
                    ),
                    RaftState.Leader(
                        State(
                            THIS_RAFT_ID,
                            currentTerm = 1,
                        ),
                        LeaderState()
                    ),
                )
            ) { raftState ->
                verifyTransition(
                    raftState,
                    RaftEvent.RequestVoteRPC(
                        candidateTerm = 2,
                        candidateId = OTHER_RAFT_ID,
                        lastLogIndex = 0,
                        lastLogTerm = 0
                    ),
                    RaftState.Follower(
                        State(THIS_RAFT_ID, currentTerm = 2, votedFor = OTHER_RAFT_ID)
                    ),
                    RaftSideEffect.RequestVoteRPCResponse(clientTerm = 2, true)
                )
            }
        }

        test("Follower becomes candidate on timeout") {
            val stateMachine =
                buildArbitraryRaftStateMachine(RaftState.Follower(State(THIS_RAFT_ID)))
            val transition =
                stateMachine.transition(RaftEvent.FollowerTimeOut) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Candidate(
                    State(id = THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)
                )
            transition.sideEffect shouldBe
                RaftSideEffect.StartRequestVoteRPCRequest(
                    State(id = THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)
                )
        }

        test("Candidate becomes candidate on timeout") {
            val stateMachine =
                buildArbitraryRaftStateMachine(
                    RaftState.Candidate(
                        State(THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)
                    )
                )
            val transition =
                stateMachine.transition(RaftEvent.CandidateElectionTimeOut) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Candidate(
                    State(id = THIS_RAFT_ID, currentTerm = 2, votedFor = THIS_RAFT_ID)
                )
            transition.sideEffect shouldBe
                RaftSideEffect.StartRequestVoteRPCRequest(
                    State(id = THIS_RAFT_ID, currentTerm = 2, votedFor = THIS_RAFT_ID)
                )
        }

        test("Candidate becomes leader on election win") {
            val stateMachine =
                buildArbitraryRaftStateMachine(
                    RaftState.Candidate(
                        State(THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)
                    )
                )
            val transition =
                stateMachine.transition(RaftEvent.CandidateMajorityVotesReceived) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Leader(
                    State(id = THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID),
                    LeaderState()
                )
            transition.sideEffect shouldBe
                RaftSideEffect.StartAppendEntriesRPCRequest(
                    State(id = THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)
                )
        }

        test("Candidate converts to follower if response is of higher term") {
            val stateMachine =
                buildArbitraryRaftStateMachine(
                    RaftState.Candidate(
                        State(THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)
                    )
                )
            val transition =
                stateMachine.transition(
                    RaftEvent.RequestVoteRPCResponse(
                        RaftSideEffect.RequestVoteRPCResponse(clientTerm = 2, voteGranted = false)
                    )
                ) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Follower(
                    State(id = THIS_RAFT_ID, currentTerm = 2),
                )
            transition.sideEffect shouldBe null
        }

        test("Leader converts to follower if response is of higher term") {
            val stateMachine =
                buildArbitraryRaftStateMachine(
                    RaftState.Leader(
                        State(THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID),
                        LeaderState()
                    )
                )
            val transition =
                stateMachine.transition(
                    RaftEvent.AppendEntriesRPCResponse(
                        RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, success = false)
                    )
                ) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Follower(
                    State(id = THIS_RAFT_ID, currentTerm = 2),
                )
            transition.sideEffect shouldBe null
        }
    })
