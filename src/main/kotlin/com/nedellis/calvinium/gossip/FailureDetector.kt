package com.nedellis.calvinium.gossip

import java.net.URI
import java.time.Duration
import java.time.Instant
import java.util.UUID
import mu.KotlinLogging

internal val T_FAIL = Duration.ofSeconds(60L)
internal val T_CLEANUP = T_FAIL.multipliedBy(2L)

internal enum class PeerCondition {
    ALIVE,
    FAILED
}

internal data class PeerState(
    val address: URI,
    val heartbeat: Int,
    val lastLive: Instant,
    val condition: PeerCondition
)

internal data class State(val peers: Map<UUID, PeerState>) {
    private val logger = KotlinLogging.logger {}

    fun updatePeers(now: Instant): State {
        val newPeers: List<Pair<UUID, PeerState>> =
            peers
                .map {
                    val timeSinceLive = Duration.between(it.value.lastLive, now)
                    assert(!timeSinceLive.isNegative)

                    if (timeSinceLive < T_FAIL) {
                        it.key to it.value
                    } else if (timeSinceLive < T_CLEANUP) {
                        logger.warn { "Marking $it as FAILED due to $timeSinceLive > $T_FAIL" }
                        it.key to it.value.copy(condition = PeerCondition.FAILED)
                    } else {
                        logger.warn { "Removing $it due to $timeSinceLive > $T_CLEANUP" }
                        null
                    }
                }
                .filterNotNull()

        return this.copy(peers = newPeers.toMap())
    }

    fun mergePeers(othersPeers: Map<UUID, PeerState>): State {
        val newPeers =
            (peers.asSequence() + othersPeers.asSequence())
                .groupBy({ it.key }, { it.value })
                .mapValues { entry -> entry.value.maxByOrNull { it.heartbeat }!! }
                .toMap()

        return this.copy(peers = newPeers)
    }
}

class FailureDetector {}
