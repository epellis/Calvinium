package com.nedellis.calvinium

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.UUID

class IntegrationSuite :
    FunSpec({
        test("E2E Test 1 replica, 1 partition") {
            val localExecutor = LocalExecutorServer()
            localExecutor.setAllReplicas(mapOf(UUID.randomUUID() to localExecutor))

            val sequencer = Sequencer(Scheduler(Executor(localExecutor)))

            val key = RecordKey(0, 0)

            sequencer.run(Transaction(listOf(Operation(key, Get)))) shouldBe null
            sequencer.run(Transaction(listOf(Operation(key, Put("B"))))) shouldBe "B"
            sequencer.run(Transaction(listOf(Operation(key, Get)))) shouldBe "B"
            sequencer.run(Transaction(listOf(Operation(key, Delete)))) shouldBe "B"
            sequencer.run(Transaction(listOf(Operation(key, Get)))) shouldBe null
        }
    })
