package com.nedellis.calvinium

import com.google.common.util.concurrent.ServiceManager
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.UUID

class IntegrationSuite : FunSpec() {
    // TODO: Specify which replica mutations are executed on
    // TODO: Test transactions
    private data class OperationTestCase(val op: Operation, val result: RecordValue)

    private suspend fun assertTransaction(
        transactions: List<OperationTestCase>,
        replicas: Int = 1
    ) {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE")

        val localExecutors =
            (0 until replicas).map { idx -> LocalExecutorServer(UUID(0, idx.toLong())) }
        for (ex in localExecutors) {
            ex.initAllPartitions(localExecutors.associateBy { it.partitionUUID })
        }
        val sequencers = localExecutors.map { LocalSequencerService(Scheduler(Executor(it))) }

        for (seq in sequencers) {
            seq.setAllSequencers(sequencers)
        }

        val serviceManager = ServiceManager(sequencers)
        serviceManager.startAsync()
        serviceManager.awaitHealthy()

        for (txnTest in transactions) {
            when (txnTest.op.type) {
                Get ->
                    for (seq in sequencers) {
                        seq.executeTxn(Transaction(listOf(txnTest.op))) shouldBe txnTest.result
                    }
                is Put, Delete ->
                    sequencers[0].executeTxn(Transaction(listOf(txnTest.op))) shouldBe
                        txnTest.result
            }
        }
    }

    init {
        test("E2E Test 1 replica, 1 partition").config(testCoroutineDispatcher = true) {
            val key = RecordKey(0, 0)

            val testCases =
                listOf(
                    OperationTestCase(Operation(key, Get), RecordValue()),
                    OperationTestCase(Operation(key, Put("B")), RecordValue("B")),
                    OperationTestCase(Operation(key, Get), RecordValue("B")),
                    OperationTestCase(Operation(key, Delete), RecordValue("B")),
                    OperationTestCase(Operation(key, Get), RecordValue()),
                )

            assertTransaction(testCases, 1)
        }

        test("E2E Test 1 replica, 2 partition").config(testCoroutineDispatcher = true) {
            val key = RecordKey(0, 0)

            val testCases =
                listOf(
                    OperationTestCase(Operation(key, Get), RecordValue()),
                    OperationTestCase(Operation(key, Put("B")), RecordValue("B")),
                    OperationTestCase(Operation(key, Get), RecordValue("B")),
                    OperationTestCase(Operation(key, Delete), RecordValue("B")),
                    OperationTestCase(Operation(key, Get), RecordValue()),
                )

            assertTransaction(testCases, 2)
        }

        test("E2E Test 1 replica, 5 partition").config(testCoroutineDispatcher = true) {
            val key = RecordKey(0, 0)

            val testCases =
                listOf(
                    OperationTestCase(Operation(key, Get), RecordValue()),
                    OperationTestCase(Operation(key, Put("B")), RecordValue("B")),
                    OperationTestCase(Operation(key, Get), RecordValue("B")),
                    OperationTestCase(Operation(key, Delete), RecordValue("B")),
                    OperationTestCase(Operation(key, Get), RecordValue()),
                )

            assertTransaction(testCases, 5)
        }
    }
}