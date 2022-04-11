package com.nedellis.calvinium

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class IntegrationSuite : FunSpec({
    test("E2E Test") {
        val sequencer = Sequencer(Scheduler(Executor()))

        val key = RecordKey(0, 0)

        sequencer.run(Transaction(listOf(Operation(key, Get)))) shouldBe null
        sequencer.run(Transaction(listOf(Operation(key, Put("B"))))) shouldBe "B"
        sequencer.run(Transaction(listOf(Operation(key, Get)))) shouldBe "B"
        sequencer.run(Transaction(listOf(Operation(key, Delete)))) shouldBe "B"
        sequencer.run(Transaction(listOf(Operation(key, Get)))) shouldBe null
    }
})