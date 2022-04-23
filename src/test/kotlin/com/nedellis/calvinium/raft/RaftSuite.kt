package com.nedellis.calvinium.raft

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
        context("Always convert to follower when receiving message of higher term") {
            withData(
                listOf(
                    //                    RaftEvent.RequestVoteRPC(
                    //                        candidateTerm = 2,
                    //                        candidateId = OTHER_RAFT_ID,
                    //                        lastLogIndex = 0,
                    //                        lastLogTerm = 0
                    //                    ),
                    RaftEvent.AppendEntriesRPC(
                        leaderTerm = 2,
                        leaderId = OTHER_RAFT_ID,
                        prevLogIndex = 0,
                        entries = listOf(),
                        leaderCommitIndex = 0
                    )
                )
            ) { raftEvent ->
                withData(
                    listOf(
                        RaftState.Follower(State(THIS_RAFT_ID, currentTerm = 1)),
                        RaftState.Candidate(State(THIS_RAFT_ID, currentTerm = 1)),
                        RaftState.Leader(State(THIS_RAFT_ID, currentTerm = 1), LeaderState()),
                    )
                ) { raftState ->
                    verifyTransition(
                        raftState,
                        raftEvent,
                        RaftState.Follower(State(THIS_RAFT_ID, currentTerm = 2)),
                        RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, true)
                    )
                }
            }
        }

        test("Follower becomes candidate on timeout") {
            val id = UUID.nameUUIDFromBytes(byteArrayOf(0, 0))
            val stateMachine = buildArbitraryRaftStateMachine(RaftState.Follower(State(id)))
            val transition =
                stateMachine.transition(RaftEvent.FollowerTimeOut) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Candidate(State(id = id, currentTerm = 1, votedFor = id))
            transition.sideEffect shouldBe
                RaftSideEffect.StartRequestVoteRPCRequest(
                    State(id = id, currentTerm = 1, votedFor = id)
                )
        }

        test("Candidate becomes candidate on timeout") {
            val id = UUID.nameUUIDFromBytes(byteArrayOf(0, 0))
            val stateMachine =
                buildArbitraryRaftStateMachine(
                    RaftState.Candidate(State(id, currentTerm = 1, votedFor = id))
                )
            val transition =
                stateMachine.transition(RaftEvent.CandidateElectionTimeOut) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Candidate(State(id = id, currentTerm = 2, votedFor = id))
            transition.sideEffect shouldBe
                RaftSideEffect.StartRequestVoteRPCRequest(
                    State(id = id, currentTerm = 2, votedFor = id)
                )
        }

        test("Candidate becomes leader on election win") {
            val id = UUID.nameUUIDFromBytes(byteArrayOf(0, 0))
            val stateMachine =
                buildArbitraryRaftStateMachine(
                    RaftState.Candidate(State(id, currentTerm = 1, votedFor = id))
                )
            val transition =
                stateMachine.transition(RaftEvent.CandidateMajorityVotesReceived) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Leader(State(id = id, currentTerm = 1, votedFor = id), LeaderState())
            transition.sideEffect shouldBe
                RaftSideEffect.StartAppendEntriesRPCRequest(
                    State(id = id, currentTerm = 1, votedFor = id)
                )
        }

        //        test("Candidate becomes follower on leader discovery") {
        //            val id = UUID.nameUUIDFromBytes(byteArrayOf(0, 0))
        //            val stateMachine =
        //                buildArbitraryRaftStateMachine(
        //                    RaftState.Candidate(State(id, currentTerm = 1, votedFor = id))
        //                )
        //            val transition =
        //                stateMachine.transition(RaftEvent.DiscoverHigherTerm(2)) as
        //                    StateMachine.Transition.Valid<*, *, *>
        //            stateMachine.state shouldBe
        //                RaftState.Follower(State(id = id, currentTerm = 2, votedFor = null))
        //            transition.sideEffect shouldBe null
        //        }
        //
        //        test("Leader becomes follower on higher term discovery") {
        //            val id = UUID.nameUUIDFromBytes(byteArrayOf(0, 0))
        //            val stateMachine =
        //                buildArbitraryRaftStateMachine(
        //                    RaftState.Leader(State(id, currentTerm = 1, votedFor = id),
        // LeaderState())
        //                )
        //            val transition =
        //                stateMachine.transition(RaftEvent.DiscoverHigherTerm(2)) as
        //                    StateMachine.Transition.Valid<*, *, *>
        //            stateMachine.state shouldBe
        //                RaftState.Follower(State(id = id, currentTerm = 2, votedFor = null))
        //            transition.sideEffect shouldBe null
        //        }
    })
