package com.nedellis.calvinium

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class IntegrationSuite : FunSpec({
    test("E2E Test") {
        val sequencer = Sequencer(Scheduler(Executor()))

        sequencer.run(Operation("A", Put("B"))) shouldBe "B"
        sequencer.run(Operation("A", Put("B"))) shouldBe "B"
        sequencer.run(Operation("A", Get)) shouldBe "B"
        sequencer.run(Operation("A", Delete)) shouldBe "B"
    }
})