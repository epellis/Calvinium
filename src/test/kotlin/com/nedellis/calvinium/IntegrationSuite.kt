package com.nedellis.calvinium

import com.google.common.util.concurrent.ServiceManager
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

class IntegrationSuite : FunSpec() {
    // TODO: Specify which replica mutations are executed on
    // TODO: Test transactions
    private data class OperationTestCase(val op: Operation, val result: RecordValue)

    private suspend fun assertTransaction(
        transactions: List<OperationTestCase>,
        replicas: Int = 1
    ) {
        val localExecutors =
            (0 until replicas).map { idx -> LocalExecutorServer(UUID(0, idx.toLong())) }
        for (ex in localExecutors) {
            ex.initAllPartitions(localExecutors.associateBy { it.partitionUUID })
        }
        val sequencers = localExecutors.map { LocalSequencerService(Scheduler(Executor(it))) }

        for (seq in sequencers) {
            seq.setAllSequencers(sequencers)
        }

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
        test("E2E Test 1 replica, 1 partition, mk2").config(timeout = 1000.milliseconds) {
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

        test("E2E Test 1 replica, 1 partition").config(testCoroutineDispatcher = true) {
            val localExecutors = listOf(LocalExecutorServer(UUID.randomUUID()))
            for (ex in localExecutors) {
                ex.initAllPartitions(localExecutors.associateBy { it.partitionUUID })
            }
            val sequencers = localExecutors.map { LocalSequencerService(Scheduler(Executor(it))) }

            for (seq in sequencers) {
                seq.setAllSequencers(listOf())
            }

            val serviceManager = ServiceManager(sequencers)
            serviceManager.startAsync()
            serviceManager.awaitHealthy()

            val key = RecordKey(0, 0)

            runBlocking {
                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Get)))) shouldBe
                    RecordValue()
                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Put("B"))))) shouldBe
                    RecordValue("B")
                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Get)))) shouldBe
                    RecordValue("B")
                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Delete)))) shouldBe
                    RecordValue("B")
                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Get)))) shouldBe
                    RecordValue()
            }

            serviceManager.stopAsync()
            serviceManager.awaitStopped()
        }

        test("E2E Test 1 replica, 2 partition").config(testCoroutineDispatcher = true) {
            val localExecutors =
                listOf(LocalExecutorServer(UUID(0, 0)), LocalExecutorServer(UUID(0, 1)))
            for (ex in localExecutors) {
                ex.initAllPartitions(localExecutors.associateBy { it.partitionUUID })
            }
            val sequencers = localExecutors.map { LocalSequencerService(Scheduler(Executor(it))) }

            sequencers[0].setAllSequencers(sequencers)
            sequencers[1].setAllSequencers(sequencers)

            val serviceManager = ServiceManager(sequencers)
            serviceManager.startAsync()
            serviceManager.awaitHealthy()

            val key = RecordKey(0, 0)

            runBlocking {
                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Get)))) shouldBe
                    RecordValue()
                sequencers[1].executeTxn(Transaction(listOf(Operation(key, Get)))) shouldBe
                    RecordValue()

                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Put("B"))))) shouldBe
                    RecordValue("B")

                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Get)))) shouldBe
                    RecordValue("B")
                sequencers[1].executeTxn(Transaction(listOf(Operation(key, Get)))) shouldBe
                    RecordValue("B")

                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Delete)))) shouldBe
                    RecordValue("B")

                sequencers[0].executeTxn(Transaction(listOf(Operation(key, Get)))) shouldBe
                    RecordValue()
                sequencers[1].executeTxn(Transaction(listOf(Operation(key, Get)))) shouldBe
                    RecordValue()
            }

            serviceManager.stopAsync()
            serviceManager.awaitStopped()
        }
    }
}
