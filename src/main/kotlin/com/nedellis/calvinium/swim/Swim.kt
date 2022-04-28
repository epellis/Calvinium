package com.nedellis.calvinium.swim

import com.tinder.StateMachine
import java.time.Duration
import java.util.UUID

internal val FAILURE_DETECTOR_PERIOD: Duration = Duration.ofMinutes(10)
internal const val FAILURE_DETECTOR_SUBGROUP_SIZE: Int = 1

internal sealed class PeerState {
    abstract val id: UUID

    data class Alive(override val id: UUID) : PeerState()
    data class PingPending(override val id: UUID) : PeerState()
    data class IndirectPingPending(override val id: UUID) : PeerState()
    data class Failed(override val id: UUID) : PeerState()
}

internal sealed interface Event {
    object PingMember : Event
    object PingTimeout : Event
    object PingReturned : Event
}

internal sealed interface SideEffect {
    data class SendPing(val id: UUID) : SideEffect
    data class SendIndirectPing(val id: UUID) : SideEffect
    data class BroadcastFailure(val id: UUID) : SideEffect
}

internal fun buildArbitrarySwimPeerStateMachine(
    initialState: PeerState
): StateMachine<PeerState, Event, SideEffect> {
    return StateMachine.create {
        initialState(initialState)

        state<PeerState.Alive> {
            on<Event.PingMember> {
                transitionTo(PeerState.PingPending(id), SideEffect.SendPing(id))
            }

            on<Event.PingReturned> { transitionTo(this) }
        }

        state<PeerState.PingPending> {
            on<Event.PingTimeout> {
                transitionTo(PeerState.IndirectPingPending(id), SideEffect.SendIndirectPing(id))
            }

            on<Event.PingReturned> { transitionTo(PeerState.Alive(id)) }
        }

        state<PeerState.IndirectPingPending> {
            on<Event.PingTimeout> {
                transitionTo(PeerState.Failed(id), SideEffect.BroadcastFailure(id))
            }

            on<Event.PingReturned> { transitionTo(PeerState.Alive(id)) }
        }

        state<PeerState.Failed> {}

        onTransition {
            val validTransition = it as? StateMachine.Transition.Valid ?: return@onTransition
            when (validTransition.sideEffect) {
                else -> {}
            }
        }
    }
}
