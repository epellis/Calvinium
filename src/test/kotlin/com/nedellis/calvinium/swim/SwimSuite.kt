package com.nedellis.calvinium.swim

import com.tinder.StateMachine
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.*

private fun verifyTransition(
    startingState: PeerState,
    event: Event,
    endingState: PeerState,
    sideEffect: SideEffect?
) {
    val stateMachine = buildArbitrarySwimPeerStateMachine(startingState)
    val transition = stateMachine.transition(event) as StateMachine.Transition.Valid<*, *, *>
    stateMachine.state shouldBe endingState
    transition.sideEffect shouldBe sideEffect
}

private val THIS_PEER_ID = UUID.nameUUIDFromBytes(byteArrayOf(0, 0))

class SwimSuite :
    FunSpec({
        test("Alive transitions to ping pending") {
            verifyTransition(
                PeerState.Alive(THIS_PEER_ID),
                Event.PingMember,
                PeerState.PingPending(THIS_PEER_ID),
                SideEffect.SendPing(THIS_PEER_ID)
            )
        }

        test("Alive transitions to alive") {
            verifyTransition(
                PeerState.Alive(THIS_PEER_ID),
                Event.PingReturned,
                PeerState.Alive(THIS_PEER_ID),
                null
            )
        }

        test("Ping pending transitions to ping timeout") {
            verifyTransition(
                PeerState.PingPending(THIS_PEER_ID),
                Event.PingTimeout,
                PeerState.IndirectPingPending(THIS_PEER_ID),
                SideEffect.SendIndirectPing(THIS_PEER_ID)
            )
        }

        test("Ping pending transitions to alive") {
            verifyTransition(
                PeerState.PingPending(THIS_PEER_ID),
                Event.PingReturned,
                PeerState.Alive(THIS_PEER_ID),
                null
            )
        }

        test("Indirect ping pending transitions to failed") {
            verifyTransition(
                PeerState.IndirectPingPending(THIS_PEER_ID),
                Event.PingTimeout,
                PeerState.Failed(THIS_PEER_ID),
                SideEffect.BroadcastFailure(THIS_PEER_ID)
            )
        }

        test("Indirect ping pending transitions to alive") {
            verifyTransition(
                PeerState.IndirectPingPending(THIS_PEER_ID),
                Event.PingReturned,
                PeerState.Alive(THIS_PEER_ID),
                null
            )
        }
    })
