package com.nedellis.calvinium.swim

import com.google.common.util.concurrent.AbstractExecutionThreadService
import com.nedellis.calvinium.proto.SwimGossip
import com.nedellis.calvinium.proto.swimGossip
import com.tinder.StateMachine
import io.grpc.Channel
import io.grpc.ManagedChannelBuilder
import java.time.Duration
import java.util.UUID
import kotlin.random.Random
import kotlin.time.toKotlinDuration
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging

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

internal data class SwimServiceState(
    val otherMembers: Map<UUID, StateMachine<PeerState, Event, SideEffect>>
) {
    private val logger = KotlinLogging.logger {}
}

internal class SwimService : AbstractExecutionThreadService() {
    private val logger = KotlinLogging.logger {}

    private val lock = Mutex()
    private val state = SwimServiceState(otherMembers = mapOf())

    private var connections = mapOf<UUID, Channel>()

    private fun buildGossip(): SwimGossip {
        return swimGossip {}
    }

    private fun connectionForId(id: UUID): Channel {
        val conn =
            connections.getOrElse(id) {
                ManagedChannelBuilder.forAddress("", 0).usePlaintext().build()
            }

        connections = connections.plus(id to conn)
        return conn
    }

    override fun run() {
        runBlocking {
            while (isRunning) {
                delay(FAILURE_DETECTOR_PERIOD.toKotlinDuration())
                lock.withLock {
                    val livePeers = state.otherMembers.filterValues { it.state is PeerState.Alive }

                    if (livePeers.isNotEmpty()) {
                        val (id, state) = livePeers.toList()[Random.nextInt(livePeers.size - 1)]
                        val rpc =
                            state.transition(Event.PingMember) as
                                StateMachine.Transition.Valid<*, *, *>
                    } else {
                        logger.debug {
                            "Skipping broadcast, no state in ${state.otherMembers.entries} was live"
                        }
                    }
                }
            }
        }
    }
}
