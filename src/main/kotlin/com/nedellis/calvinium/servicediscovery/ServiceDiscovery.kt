package com.nedellis.calvinium.servicediscovery

import java.util.UUID

data class Peer(val id: UUID, val address: String)

interface ServiceDiscovery {
    fun getPeers(): List<Peer>
}
