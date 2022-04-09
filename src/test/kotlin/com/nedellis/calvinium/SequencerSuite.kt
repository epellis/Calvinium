package com.nedellis.calvinium

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify

class SequencerSuite : FunSpec({
    test("Sequencer runs op") {
        val mockScheduler = mockk<Scheduler>()
        every { mockScheduler.run(any()) } returns "RESULT"

        val sequencer = Sequencer(mockScheduler)
        val res = sequencer.run(Operation("KEY", Get))

        res shouldBe "RESULT"

        verify { mockScheduler.run(any()) }
        confirmVerified(mockScheduler)
    }
})