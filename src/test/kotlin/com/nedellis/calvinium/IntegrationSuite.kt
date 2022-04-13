package com.nedellis.calvinium

import com.google.common.util.concurrent.ServiceManager
import io.kotest.common.runBlocking
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.util.UUID

class IntegrationSuite : FunSpec() {
    init {
        test("E2E Test 1 replica, 1 partition").config(testCoroutineDispatcher = true) {
            val localExecutors = listOf(LocalExecutorServer(UUID.randomUUID()))
            for (ex in localExecutors) {
                ex.setAllPartitions(localExecutors.associateBy { it.partitionUUID })
            }
            val sequencers = localExecutors.map { LocalSequencerService(Scheduler(Executor(it))) }

            for (seq in sequencers) {
                seq.setOtherSequencers(listOf())
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
                ex.setAllPartitions(localExecutors.associateBy { it.partitionUUID })
            }
            val sequencers = localExecutors.map { LocalSequencerService(Scheduler(Executor(it))) }

            sequencers[0].setOtherSequencers(listOf(sequencers[1]))
            sequencers[1].setOtherSequencers(listOf(sequencers[0]))

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