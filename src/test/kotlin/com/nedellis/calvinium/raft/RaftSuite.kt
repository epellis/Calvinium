package com.nedellis.calvinium.raft

import com.tinder.StateMachine
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.UUID

class RaftSuite :
    FunSpec({
        test("Follower becomes candidate on timeout") {
            val id = UUID.nameUUIDFromBytes(byteArrayOf(0, 0))
            val stateMachine = buildArbitraryRaftStateMachine(RaftState.Follower(State(id)))
            val transition =
                stateMachine.transition(RaftEvent.FollowerTimeOut) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Candidate(State(id = id, currentTerm = 1, votedFor = id))
            transition.sideEffect shouldBe
                RaftSideEffect.StartElection(State(id = id, currentTerm = 1, votedFor = id))
        }

        test("Candidate becomes candidate on timeout") {
            val id = UUID.nameUUIDFromBytes(byteArrayOf(0, 0))
            val stateMachine =
                buildArbitraryRaftStateMachine(
                    RaftState.Candidate(State(id, currentTerm = 1, votedFor = id)))
            val transition =
                stateMachine.transition(RaftEvent.CandidateElectionTimeOut) as
                    StateMachine.Transition.Valid<*, *, *>
            stateMachine.state shouldBe
                RaftState.Candidate(State(id = id, currentTerm = 2, votedFor = id))
            transition.sideEffect shouldBe
                RaftSideEffect.StartElection(State(id = id, currentTerm = 2, votedFor = id))
        }
    })
