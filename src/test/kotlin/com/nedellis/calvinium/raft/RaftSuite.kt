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
            verifyTransition(
                RaftState.Follower(State(THIS_RAFT_ID, currentTerm = 0)),
                RaftEvent.FollowerTimeOut(listOf(THIS_RAFT_ID)),
                RaftState.Candidate(
                    State(id = THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)
                ),
                RaftSideEffect.StartRequestVoteRPCRequest(
                    mapOf(
                        THIS_RAFT_ID to
                            RaftEvent.RequestVoteRPC(candidateTerm = 1, candidateId = THIS_RAFT_ID)
                    )
                )
            )
        }

        test("Candidate becomes candidate on timeout") {
            verifyTransition(
                RaftState.Candidate(State(THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)),
                RaftEvent.CandidateElectionTimeOut(listOf(THIS_RAFT_ID)),
                RaftState.Candidate(
                    State(id = THIS_RAFT_ID, currentTerm = 2, votedFor = THIS_RAFT_ID)
                ),
                RaftSideEffect.StartRequestVoteRPCRequest(
                    mapOf(
                        THIS_RAFT_ID to
                            RaftEvent.RequestVoteRPC(candidateTerm = 2, candidateId = THIS_RAFT_ID)
                    )
                )
            )
        }

        test("Candidate becomes leader on election win") {
            verifyTransition(
                RaftState.Candidate(State(THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)),
                RaftEvent.CandidateMajorityVotesReceived(listOf(THIS_RAFT_ID, OTHER_RAFT_ID)),
                RaftState.Leader(
                    State(id = THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID),
                    LeaderState(
                        nextIndex = mapOf(THIS_RAFT_ID to 1, OTHER_RAFT_ID to 1),
                        matchIndex = mapOf(THIS_RAFT_ID to 0, OTHER_RAFT_ID to 0)
                    )
                ),
                RaftSideEffect.StartAppendEntriesRPCRequest(
                    mapOf(
                        THIS_RAFT_ID to
                            RaftEvent.AppendEntriesRPC(leaderTerm = 1, leaderId = THIS_RAFT_ID),
                        OTHER_RAFT_ID to
                            RaftEvent.AppendEntriesRPC(leaderTerm = 1, leaderId = THIS_RAFT_ID)
                    )
                )
            )
        }

        test("Candidate converts to follower if response is of higher term") {
            verifyTransition(
                RaftState.Candidate(State(THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID)),
                RaftEvent.RequestVoteRPCResponse(
                    RaftSideEffect.RequestVoteRPCResponse(clientTerm = 2, voteGranted = false)
                ),
                RaftState.Follower(
                    State(id = THIS_RAFT_ID, currentTerm = 2),
                ),
                null
            )
        }

        test("Leader converts to follower if response is of higher term") {
            verifyTransition(
                RaftState.Leader(
                    State(THIS_RAFT_ID, currentTerm = 1, votedFor = THIS_RAFT_ID),
                    LeaderState()
                ),
                RaftEvent.AppendEntriesRPCResponse(
                    OTHER_RAFT_ID,
                    RaftEvent.AppendEntriesRPC(leaderTerm = 1, leaderId = THIS_RAFT_ID),
                    RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, success = false)
                ),
                RaftState.Follower(
                    State(id = THIS_RAFT_ID, currentTerm = 2),
                ),
                null
            )
        }

        test("Append entries fails if no entry at prev log index") {
            verifyTransition(
                RaftState.Follower(State(THIS_RAFT_ID, currentTerm = 2)),
                RaftEvent.AppendEntriesRPC(
                    leaderTerm = 2,
                    leaderId = OTHER_RAFT_ID,
                    prevLogIndex = 1,
                    prevLogTerm = 0,
                    leaderCommitIndex = 0
                ),
                RaftState.Follower(State(THIS_RAFT_ID, currentTerm = 2)),
                RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, success = false)
            )
        }

        test("Append entries fails if prev log index has incorrect term") {
            verifyTransition(
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        commitIndex = 0,
                        currentTerm = 2,
                        log = ImmutableList.of(LogEntry(2, null))
                    )
                ),
                RaftEvent.AppendEntriesRPC(
                    leaderTerm = 2,
                    leaderId = OTHER_RAFT_ID,
                    prevLogIndex = 0,
                    prevLogTerm = 1,
                    leaderCommitIndex = 0
                ),
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        commitIndex = 0,
                        currentTerm = 2,
                        log = ImmutableList.of(LogEntry(2, null))
                    )
                ),
                RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, success = false)
            )
        }

        test("Append entries succeeds and doesn't rewrite the log") {
            verifyTransition(
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        currentTerm = 1,
                    )
                ),
                RaftEvent.AppendEntriesRPC(
                    leaderTerm = 1,
                    leaderId = OTHER_RAFT_ID,
                    prevLogIndex = 0,
                    prevLogTerm = 0,
                    entries = ImmutableList.of(LogEntry(1, null))
                ),
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        currentTerm = 1,
                        log = ImmutableList.of(LogEntry(0, null), LogEntry(1, null))
                    )
                ),
                RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 1, success = true)
            )
        }

        test("Append entries succeeds and rewrites the log 1") {
            verifyTransition(
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        currentTerm = 2,
                        log = ImmutableList.of(LogEntry(0, null), LogEntry(1, null))
                    )
                ),
                RaftEvent.AppendEntriesRPC(
                    leaderTerm = 2,
                    leaderId = OTHER_RAFT_ID,
                    prevLogIndex = 0,
                    prevLogTerm = 0,
                    entries = ImmutableList.of(LogEntry(2, null))
                ),
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        currentTerm = 2,
                        log = ImmutableList.of(LogEntry(0, null), LogEntry(2, null))
                    )
                ),
                RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, success = true)
            )
        }

        test("Append entries succeeds and rewrites the log 2") {
            verifyTransition(
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        currentTerm = 2,
                        log =
                            ImmutableList.of(
                                LogEntry(0, null),
                                LogEntry(1, null),
                                LogEntry(1, null)
                            )
                    )
                ),
                RaftEvent.AppendEntriesRPC(
                    leaderTerm = 2,
                    leaderId = OTHER_RAFT_ID,
                    prevLogIndex = 0,
                    prevLogTerm = 0,
                    entries = ImmutableList.of(LogEntry(1, null), LogEntry(2, null))
                ),
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        currentTerm = 2,
                        log =
                            ImmutableList.of(
                                LogEntry(0, null),
                                LogEntry(1, null),
                                LogEntry(2, null)
                            )
                    )
                ),
                RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, success = true)
            )
        }

        test("Append Entries updates commit index to index of last entry") {
            verifyTransition(
                RaftState.Follower(
                    State(THIS_RAFT_ID, currentTerm = 2, log = ImmutableList.of(LogEntry(1, null)))
                ),
                RaftEvent.AppendEntriesRPC(
                    leaderTerm = 2,
                    leaderId = OTHER_RAFT_ID,
                    prevLogIndex = 0,
                    prevLogTerm = 1,
                    leaderCommitIndex = 1,
                    entries = ImmutableList.of()
                ),
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        currentTerm = 2,
                        commitIndex = 0,
                        log = ImmutableList.of(LogEntry(1, null))
                    )
                ),
                RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, success = true)
            )
        }

        // TODO Make sure this is consistent with the starting state of log
        test("Append Entries updates commit index to index of last entry") {
            verifyTransition(
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        currentTerm = 2,
                        log = ImmutableList.of(LogEntry(1, null), LogEntry(1, null))
                    )
                ),
                RaftEvent.AppendEntriesRPC(
                    leaderTerm = 2,
                    leaderId = OTHER_RAFT_ID,
                    prevLogIndex = 0,
                    prevLogTerm = 1,
                    leaderCommitIndex = 0,
                    entries = ImmutableList.of()
                ),
                RaftState.Follower(
                    State(
                        THIS_RAFT_ID,
                        currentTerm = 2,
                        commitIndex = 0,
                        log = ImmutableList.of(LogEntry(1, null), LogEntry(1, null))
                    )
                ),
                RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 2, success = true)
            )
        }

        test("Append Entries successful response updates entries in leader state") {
            verifyTransition(
                RaftState.Leader(
                    State(THIS_RAFT_ID, currentTerm = 1, log = ImmutableList.of(LogEntry(1, null))),
                    LeaderState(
                        nextIndex = mapOf(THIS_RAFT_ID to 1, OTHER_RAFT_ID to 1),
                        matchIndex = mapOf(THIS_RAFT_ID to 0, OTHER_RAFT_ID to 0)
                    )
                ),
                RaftEvent.AppendEntriesRPCResponse(
                    OTHER_RAFT_ID,
                    RaftEvent.AppendEntriesRPC(
                        leaderTerm = 1,
                        leaderId = THIS_RAFT_ID,
                        entries = ImmutableList.of(LogEntry(1, null))
                    ),
                    RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 1, success = true)
                ),
                RaftState.Leader(
                    State(THIS_RAFT_ID, currentTerm = 1, log = ImmutableList.of(LogEntry(1, null))),
                    LeaderState(
                        nextIndex = mapOf(THIS_RAFT_ID to 1, OTHER_RAFT_ID to 2),
                        matchIndex = mapOf(THIS_RAFT_ID to 0, OTHER_RAFT_ID to 1)
                    )
                ),
                null
            )
        }

        test("Append Entries failed response walks back next index") {
            verifyTransition(
                RaftState.Leader(
                    State(THIS_RAFT_ID, currentTerm = 1, log = ImmutableList.of(LogEntry(1, null))),
                    LeaderState(
                        nextIndex = mapOf(THIS_RAFT_ID to 2, OTHER_RAFT_ID to 2),
                        matchIndex = mapOf(THIS_RAFT_ID to 1, OTHER_RAFT_ID to 1)
                    )
                ),
                RaftEvent.AppendEntriesRPCResponse(
                    OTHER_RAFT_ID,
                    RaftEvent.AppendEntriesRPC(
                        leaderTerm = 1,
                        leaderId = THIS_RAFT_ID,
                        entries = ImmutableList.of(LogEntry(1, null))
                    ),
                    RaftSideEffect.AppendEntriesRPCResponse(clientTerm = 1, success = false)
                ),
                RaftState.Leader(
                    State(THIS_RAFT_ID, currentTerm = 1, log = ImmutableList.of(LogEntry(1, null))),
                    LeaderState(
                        nextIndex = mapOf(THIS_RAFT_ID to 2, OTHER_RAFT_ID to 1),
                        matchIndex = mapOf(THIS_RAFT_ID to 1, OTHER_RAFT_ID to 1)
                    )
                ),
                RaftSideEffect.StartAppendEntriesRPCRequest(
                    mapOf(
                        OTHER_RAFT_ID to
                            RaftEvent.AppendEntriesRPC(
                                leaderTerm = 1,
                                leaderId = THIS_RAFT_ID,
                                entries = ImmutableList.of(LogEntry(1, null))
                            ),
                    )
                )
            )
        }
    })
