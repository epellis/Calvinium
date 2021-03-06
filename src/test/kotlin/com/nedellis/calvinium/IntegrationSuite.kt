package com.nedellis.calvinium

import com.google.common.util.concurrent.ServiceManager
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import io.kotest.datatest.withData
import io.kotest.matchers.shouldBe
import java.util.UUID

// TODO: Specify which replica mutations are executed on
// TODO: Test transactions
// TODO: FPGA style testing with a software model of the DB

private data class LogicTest(val op: Operation, val expectedResult: RecordValue)

private val singleKeyLifecycleTest =
    listOf(
        LogicTest(Operation(RecordKey(0, 0), Get), RecordValue()),
        LogicTest(Operation(RecordKey(0, 0), Put("B")), RecordValue("B")),
        LogicTest(Operation(RecordKey(0, 0), Get), RecordValue("B")),
        LogicTest(Operation(RecordKey(0, 0), Delete), RecordValue("B")),
        LogicTest(Operation(RecordKey(0, 0), Get), RecordValue()),
    )

private val writeDeleteWriteTest =
    listOf(
        LogicTest(Operation(RecordKey(0, 0), Put("A")), RecordValue("A")),
        LogicTest(Operation(RecordKey(0, 0), Get), RecordValue("A")),
        LogicTest(Operation(RecordKey(0, 0), Delete), RecordValue("A")),
        LogicTest(Operation(RecordKey(0, 0), Get), RecordValue()),
        LogicTest(Operation(RecordKey(0, 0), Put("B")), RecordValue("B")),
        LogicTest(Operation(RecordKey(0, 0), Get), RecordValue("B")),
    )

class LogicSuite :
    FunSpec({
        context("Test Logic") {
            withData(listOf(1, 3, 5)) { replicas ->
                withData(listOf(singleKeyLifecycleTest, writeDeleteWriteTest)) { tests ->
                    assertTransactionLogic(replicas, tests)
                }
            }
        }
    }) {
    companion object {
        private fun assertTransactionLogic(replicas: Int, tests: List<LogicTest>) {
            runBlocking {
                val localExecutors =
                    (0 until replicas).map { idx -> LocalExecutorServer(UUID(0, idx.toLong())) }
                for (ex in localExecutors) {
                    ex.initAllPartitions(localExecutors.associateBy { it.partitionUUID })
                }
                val sequencers =
                    localExecutors.map { LocalSequencerService(Scheduler(Executor(it))) }

                for (seq in sequencers) {
                    seq.setAllSequencers(sequencers)
                }

                val serviceManager = ServiceManager(sequencers)
                serviceManager.startAsync()
                serviceManager.awaitHealthy()

                for (test in tests) {
                    when (test.op.type) {
                        Get ->
                            for (seq in sequencers) {
                                seq.executeTxn(Transaction(listOf(test.op))) shouldBe
                                    test.expectedResult
                            }
                        is Put, Delete ->
                            sequencers[0].executeTxn(Transaction(listOf(test.op))) shouldBe
                                test.expectedResult
                    }
                }
            }
        }
    }
}
