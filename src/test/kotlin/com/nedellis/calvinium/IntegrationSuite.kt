package com.nedellis.calvinium

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class IntegrationSuite : FunSpec({
    test("E2E Test") {
        val sequencer = Sequencer(Scheduler(Executor()))

        sequencer.run(Transaction(listOf(Operation("A", Get)))) shouldBe null
        sequencer.run(Transaction(listOf(Operation("A", Put("B"))))) shouldBe "B"
        sequencer.run(Transaction(listOf(Operation("A", Get)))) shouldBe "B"
        sequencer.run(Transaction(listOf(Operation("A", Delete)))) shouldBe "B"
        sequencer.run(Transaction(listOf(Operation("A", Get)))) shouldBe null
    }
})