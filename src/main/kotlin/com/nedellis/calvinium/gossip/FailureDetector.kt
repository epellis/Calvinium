package com.nedellis.calvinium.gossip

import com.google.common.util.concurrent.AbstractExecutionThreadService
import com.linecorp.armeria.server.Server
import com.linecorp.armeria.server.grpc.GrpcService
import com.nedellis.calvinium.proto.GossipFDGrpc
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
    val otherAtNow = other?.updatedAtNow(now)

    return if (mine != null && otherAtNow != null) {
        if (mine.heartbeat >= otherAtNow.heartbeat) {
            mine
        } else {
            otherAtNow
        }
    } else {
        listOfNotNull(mine, otherAtNow).first()
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

internal data class FailureDetectorState(val id: UUID, val peers: Map<UUID, PeerState>) {
    private val logger = KotlinLogging.logger {}

    fun updatePeers(now: Instant): FailureDetectorState {
        assert(peers.keys.contains(id))

        val newPeers =
            peers
                .mapNotNull { peer ->
                    val stateMachine = buildStateMachine(peer.value)

                    val transition =
                        if (peer.key == id) {
                            stateMachine.transition(Event.HeartBeat(now))
                        } else {
                            stateMachine.transition(Event.Update(now))
                        } as
                            StateMachine.Transition.Valid<*, *, *>

                    if (transition.sideEffect == SideEffect.Drop) {
                        null
                    } else {
                        peer.key to stateMachine.state
                    }
                }
                .toMap()

        return this.copy(peers = newPeers)
    }

    fun mergePeers(otherPeers: Map<UUID, PeerState>, now: Instant): FailureDetectorState {
        assert(peers.keys.contains(id))

        val newPeers =
            (peers.keys + otherPeers.keys).distinct().associateWith { id ->
                mergePeer(peers[id], otherPeers[id], now)
            }

        return this.copy(peers = newPeers)
    }
}

internal class GossipFDImpl : GossipFDGrpc.GossipFDImplBase() {}

class FailureDetectorService : AbstractExecutionThreadService() {
    private val grpcService = GrpcService.builder().addService(GossipFDImpl()).build()
    private val server = Server.builder().service(grpcService).build()
    val address = server.activePort()?.localAddress()!!

    override fun run() {
        server.start()

        while (isRunning) {
            Thread.sleep(1000)
        }
    }
}
