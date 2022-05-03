package com.nedellis.calvinium.gossip

import com.tinder.StateMachine
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.time.Instant
import java.util.UUID

private val MY_ID = UUID.nameUUIDFromBytes(byteArrayOf(0, 1))
private val OTHER_ID = UUID.nameUUIDFromBytes(byteArrayOf(0, 2))

private fun verifyTransition(
    startingState: PeerState,
    event: Event,
    endingState: PeerState,
    sideEffect: SideEffect? = null
) {
    val stateMachine = buildStateMachine(startingState)
    val transition = stateMachine.transition(event) as StateMachine.Transition.Valid<*, *, *>
    stateMachine.state shouldBe endingState
    transition.sideEffect shouldBe sideEffect
}

class FailureDetectorSuite :
    FunSpec({
        test("Heartbeat") {
            verifyTransition(
                PeerState.Alive(0, Instant.ofEpochSecond(0)),
                Event.HeartBeat(Instant.ofEpochSecond(10)),
                PeerState.Alive(1, Instant.ofEpochSecond(10)),
            )
        }

        test("Update Alive -> Alive") {
            verifyTransition(
                PeerState.Alive(0, Instant.ofEpochSecond(0)),
                Event.Update(Instant.ofEpochSecond(0).plus(T_FAIL.dividedBy(2L))),
                PeerState.Alive(0, Instant.ofEpochSecond(0)),
            )
        }

        test("Update Alive -> Failed") {
            verifyTransition(
                PeerState.Alive(0, Instant.ofEpochSecond(0)),
                Event.Update(Instant.ofEpochSecond(0).plus(T_FAIL)),
                PeerState.Failed(0, Instant.ofEpochSecond(0).plus(T_FAIL)),
            )
        }

        test("Update Alive -> Failed with long time") {
            verifyTransition(
                PeerState.Alive(0, Instant.ofEpochSecond(0)),
                Event.Update(Instant.ofEpochSecond(0).plus(T_FAIL.multipliedBy(100L))),
                PeerState.Failed(0, Instant.ofEpochSecond(0).plus(T_FAIL.multipliedBy(100L))),
            )
        }

        test("Update Failed -> Failed") {
            verifyTransition(
                PeerState.Failed(0, Instant.ofEpochSecond(0)),
                Event.Update(Instant.ofEpochSecond(0).plus(T_FAIL.dividedBy(2L))),
                PeerState.Failed(0, Instant.ofEpochSecond(0)),
            )
        }

        test("Update Failed -> Dropped") {
            verifyTransition(
                PeerState.Failed(0, Instant.ofEpochSecond(0)),
                Event.Update(Instant.ofEpochSecond(0).plus(T_FAIL)),
                PeerState.Failed(0, Instant.ofEpochSecond(0)),
                SideEffect.Drop
            )
        }

        test("Update Failed -> Dropped with long time") {
            verifyTransition(
                PeerState.Failed(0, Instant.ofEpochSecond(0)),
                Event.Update(Instant.ofEpochSecond(0).plus(T_FAIL.multipliedBy(100L))),
                PeerState.Failed(0, Instant.ofEpochSecond(0)),
                SideEffect.Drop
            )
        }

        test("Merge mine and null") {
            mergePeer(
                PeerState.Alive(0, Instant.ofEpochSecond(0)),
                null,
                Instant.ofEpochSecond(0)
            ) shouldBe PeerState.Alive(0, Instant.ofEpochSecond(0))
        }

        test("Merge null and theirs") {
            mergePeer(
                null,
                PeerState.Alive(0, Instant.ofEpochSecond(0)),
                Instant.ofEpochSecond(0)
            ) shouldBe PeerState.Alive(0, Instant.ofEpochSecond(0))
        }

        test("Merge prefers mine with same heartbeat") {
            mergePeer(
                PeerState.Alive(10, Instant.ofEpochSecond(2)),
                PeerState.Alive(10, Instant.ofEpochSecond(1)),
                Instant.ofEpochSecond(0)
            ) shouldBe PeerState.Alive(10, Instant.ofEpochSecond(2))
        }

        test("Merge prefers mine with higher heartbeat") {
            mergePeer(
                PeerState.Alive(11, Instant.ofEpochSecond(0)),
                PeerState.Alive(10, Instant.ofEpochSecond(0)),
                Instant.ofEpochSecond(0)
            ) shouldBe PeerState.Alive(11, Instant.ofEpochSecond(0))
        }

        test("Merge prefers theirs with higher heartbeat") {
            mergePeer(
                PeerState.Alive(10, Instant.ofEpochSecond(0)),
                PeerState.Alive(11, Instant.ofEpochSecond(0)),
                Instant.ofEpochSecond(10)
            ) shouldBe PeerState.Alive(11, Instant.ofEpochSecond(10))
        }

        test("Merge prefers alive over failed with higher heartbeat") {
            mergePeer(
                PeerState.Alive(11, Instant.ofEpochSecond(0)),
                PeerState.Failed(10, Instant.ofEpochSecond(0)),
                Instant.ofEpochSecond(0)
            ) shouldBe PeerState.Alive(11, Instant.ofEpochSecond(0))
        }

        test("Merge prefers failed over alive with higher heartbeat") {
            mergePeer(
                PeerState.Alive(10, Instant.ofEpochSecond(0)),
                PeerState.Failed(11, Instant.ofEpochSecond(0)),
                Instant.ofEpochSecond(10)
            ) shouldBe PeerState.Failed(11, Instant.ofEpochSecond(10))
        }

        test("updatePeers heartbeats self") {
            val fd =
                FailureDetectorState(
                    MY_ID,
                    mapOf(MY_ID to PeerState.Alive(0, Instant.ofEpochSecond(0)))
                )
            val updatedFd = fd.updatePeers(Instant.ofEpochSecond(1))
            updatedFd shouldBe
                FailureDetectorState(
                    MY_ID,
                    mapOf(MY_ID to PeerState.Alive(1, Instant.ofEpochSecond(1)))
                )
        }

        test("updatePeers heartbeats self and updates other") {
            val fd =
                FailureDetectorState(
                    MY_ID,
                    mapOf(
                        MY_ID to PeerState.Alive(0, Instant.ofEpochSecond(0)),
                        OTHER_ID to PeerState.Alive(0, Instant.ofEpochSecond(0))
                    )
                )
            val updatedFd = fd.updatePeers(Instant.ofEpochSecond(0).plus(T_FAIL.dividedBy(2L)))
            updatedFd shouldBe
                FailureDetectorState(
                    MY_ID,
                    mapOf(
                        MY_ID to
                            PeerState.Alive(1, Instant.ofEpochSecond(0).plus(T_FAIL.dividedBy(2L))),
                        OTHER_ID to PeerState.Alive(0, Instant.ofEpochSecond(0))
                    )
                )
        }

        test("updatePeers heartbeats self and updates other to fail") {
            val fd =
                FailureDetectorState(
                    MY_ID,
                    mapOf(
                        MY_ID to PeerState.Alive(0, Instant.ofEpochSecond(0)),
                        OTHER_ID to PeerState.Alive(0, Instant.ofEpochSecond(0))
                    )
                )
            val updatedFd = fd.updatePeers(Instant.ofEpochSecond(0).plus(T_FAIL))
            updatedFd shouldBe
                FailureDetectorState(
                    MY_ID,
                    mapOf(
                        MY_ID to PeerState.Alive(1, Instant.ofEpochSecond(0).plus(T_FAIL)),
                        OTHER_ID to PeerState.Failed(0, Instant.ofEpochSecond(0).plus(T_FAIL))
                    )
                )
        }

        test("updatePeers heartbeats self and drops other") {
            val fd =
                FailureDetectorState(
                    MY_ID,
                    mapOf(
                        MY_ID to PeerState.Alive(0, Instant.ofEpochSecond(0)),
                        OTHER_ID to PeerState.Failed(0, Instant.ofEpochSecond(0))
                    )
                )
            val updatedFd = fd.updatePeers(Instant.ofEpochSecond(0).plus(T_FAIL))
            updatedFd shouldBe
                FailureDetectorState(
                    MY_ID,
                    mapOf(MY_ID to PeerState.Alive(1, Instant.ofEpochSecond(0).plus(T_FAIL)))
                )
        }
    })
