package com.nedellis.calvinium.gossip

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.net.URI
import java.time.Instant
import java.util.UUID

private val OTHER_PEER_ID = UUID.nameUUIDFromBytes(byteArrayOf(0, 1))

class FailureDetectorSuite :
    FunSpec({
        test("Transitions peer from alive to failed") {
            val initialState =
                State(
                    mapOf(
                        OTHER_PEER_ID to
                            PeerState(URI(""), 0, Instant.ofEpochSecond(100), PeerCondition.ALIVE)
                    )
                )
            initialState.updatePeers(Instant.ofEpochSecond(100).plus(T_FAIL)) shouldBe
                State(
                    mapOf(
                        OTHER_PEER_ID to
                            PeerState(URI(""), 0, Instant.ofEpochSecond(100), PeerCondition.FAILED)
                    )
                )
        }

        test("Transitions peer from alive to deleted") {
            val initialState =
                State(
                    mapOf(
                        OTHER_PEER_ID to
                            PeerState(URI(""), 0, Instant.ofEpochSecond(100), PeerCondition.ALIVE)
                    )
                )
            initialState.updatePeers(Instant.ofEpochSecond(100).plus(T_CLEANUP)) shouldBe
                State(mapOf())
        }

        test("Transitions peer from failed to deleted") {
            val initialState =
                State(
                    mapOf(
                        OTHER_PEER_ID to
                            PeerState(URI(""), 0, Instant.ofEpochSecond(100), PeerCondition.FAILED)
                    )
                )
            initialState.updatePeers(Instant.ofEpochSecond(100).plus(T_CLEANUP)) shouldBe
                State(mapOf())
        }

        test("Transitions peer from alive to alive") {
            val initialState =
                State(
                    mapOf(
                        OTHER_PEER_ID to
                            PeerState(URI(""), 0, Instant.ofEpochSecond(100), PeerCondition.ALIVE)
                    )
                )
            initialState.updatePeers(Instant.ofEpochSecond(100).plus(T_FAIL.dividedBy(2L))) shouldBe
                State(
                    mapOf(
                        OTHER_PEER_ID to
                            PeerState(URI(""), 0, Instant.ofEpochSecond(100), PeerCondition.ALIVE)
                    )
                )
        }
    })
