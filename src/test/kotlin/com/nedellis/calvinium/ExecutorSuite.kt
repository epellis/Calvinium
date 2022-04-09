package com.nedellis.calvinium

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import java.util.UUID

class ExecutorSuite : FunSpec({
//    test("Executor runs op") {
//        val executor = Executor()
//
//        executor.run(UniqueOperation(UUID.randomUUID(), Operation("KEY", Put("RESULT")))) shouldBe "RESULT"
//        executor.run(UniqueOperation(UUID.randomUUID(), Operation("KEY", Get))) shouldBe "RESULT"
//        executor.run(UniqueOperation(UUID.randomUUID(), Operation("KEY", Delete))) shouldBe "RESULT"
//    }
})