package com.nedellis.calvinium.gossip

import com.tinder.StateMachine
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.time.Instant

// private val OTHER_PEER_ID = UUID.nameUUIDFromBytes(byteArrayOf(0, 1))
// private val ANOTHER_PEER_ID = UUID.nameUUIDFromBytes(byteArrayOf(0, 2))

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
    })
