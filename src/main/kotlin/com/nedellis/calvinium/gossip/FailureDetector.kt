package com.nedellis.calvinium.gossip

import com.tinder.StateMachine
import java.time.Duration
import java.time.Instant
import java.util.UUID
import mu.KotlinLogging

internal val T_FAIL = Duration.ofSeconds(60L)

internal abstract class PeerState {
    abstract val heartbeat: Int
    abstract val localLastUpdated: Instant

    data class Alive(override val heartbeat: Int, override val localLastUpdated: Instant) :
        PeerState()
    data class Failed(override val heartbeat: Int, override val localLastUpdated: Instant) :
        PeerState()

    fun updatedAtNow(now: Instant): PeerState {
        return when (this) {
            is Alive -> this.copy(localLastUpdated = now)
            is Failed -> this.copy(localLastUpdated = now)
            else -> throw IllegalStateException("State must be Alive or Failed")
        }
    }
}

internal fun mergePeer(mine: PeerState?, other: PeerState?, now: Instant): PeerState {
    return if (mine != null && other != null) {
        if (mine.heartbeat >= other.heartbeat) {
            mine
        } else {
            other.updatedAtNow(now)
        }
    } else {
        listOfNotNull(mine, other).first()
    }
}

internal interface Event {
    data class HeartBeat(val now: Instant) : Event

    data class Update(val now: Instant) : Event
}

internal interface SideEffect {
    object Drop : SideEffect
}

internal typealias PeerStateMachine = StateMachine<PeerState, Event, SideEffect>

internal fun buildStateMachine(initialPeerState: PeerState): PeerStateMachine {
    val logger = KotlinLogging.logger {}

    return StateMachine.create {
        initialState(initialPeerState)
        state<PeerState.Alive> {
            on<Event.HeartBeat> {
                transitionTo(this.copy(heartbeat = this.heartbeat + 1, localLastUpdated = it.now))
            }

            on<Event.Update> {
                val duration = Duration.between(this.localLastUpdated, it.now)
                assert(!duration.isNegative)

                if (duration >= T_FAIL) {
                    logger.warn {
                        "Transitioning $this to Failed state, last updated duration $duration > T_FAIL $T_FAIL"
                    }

                    transitionTo(PeerState.Failed(this.heartbeat, it.now))
                } else {
                    transitionTo(this)
                }
            }
        }

        state<PeerState.Failed> {
            on<Event.Update> {
                val duration = Duration.between(this.localLastUpdated, it.now)
                assert(!duration.isNegative)

                if (duration >= T_FAIL) {
                    logger.warn {
                        "Deleting $this, last updated duration $duration > T_FAIL $T_FAIL"
                    }
                    transitionTo(this, sideEffect = SideEffect.Drop)
                } else {
                    transitionTo(this)
                }
            }
        }
    }
}

internal data class FailureDetectorState(val peers: Map<UUID, PeerStateMachine>) {
    private val logger = KotlinLogging.logger {}

    fun updatePeers(now: Instant): FailureDetectorState {
        val newPeers = mutableMapOf<UUID, PeerStateMachine>()

        for (peer in peers) {
            val transition =
                peer.value.transition(Event.Update(now)) as StateMachine.Transition.Valid<*, *, *>
            if (transition.sideEffect != SideEffect.Drop) {
                newPeers[peer.key] = peer.value
            }
        }

        return this.copy(peers = newPeers)
    }

    fun mergePeers(otherPeers: Map<UUID, PeerState>, now: Instant): FailureDetectorState {
        val newPeers =
            (peers.keys + otherPeers.keys).distinct().associateWith { id ->
                buildStateMachine(mergePeer(peers[id]?.state, otherPeers[id], now))
            }

        return this.copy(peers = newPeers)
    }
}

class FailureDetector {}
