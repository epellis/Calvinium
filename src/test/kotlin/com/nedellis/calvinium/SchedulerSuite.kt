package com.nedellis.calvinium

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.util.UUID

class SchedulerSuite : FunSpec({
    test("Scheduler runs op") {
        val mockExecutor = mockk<Executor>()
        every { mockExecutor.run(any()) } returns "RESULT"

        val sequencer = Scheduler(mockExecutor)
        val res = sequencer.run(UniqueOperation(UUID.randomUUID(), Operation("KEY", Get)))

        res shouldBe "RESULT"

        verify { mockExecutor.run(any()) }
        confirmVerified(mockExecutor)
    }
})