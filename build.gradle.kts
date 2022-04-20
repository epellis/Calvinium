import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.20"
    application
    id("com.google.cloud.tools.jib") version "2.3.0"
    id("com.ncorti.ktfmt.gradle") version "0.8.0"
    id("java")
}

group = "com.nedellis"

version = "1.0-SNAPSHOT"

repositories { mavenCentral() }

val KOTEST_VERSION = "5.2.3"

dependencies {
    implementation("com.google.guava:guava:31.1-jre")
    implementation("org.rocksdb:rocksdbjni:7.0.4")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1")
    implementation("io.github.microutils:kotlin-logging-jvm:2.1.21")
    implementation("org.slf4j:slf4j-simple:1.7.36")
    implementation("com.tinder.statemachine:statemachine:0.2.0")
    implementation("io.trino:trino-parser:377")

    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-runner-junit5:5.2.3")
    testImplementation("io.kotest:kotest-assertions-core:5.2.3")
    testImplementation("io.kotest:kotest-framework-datatest:5.2.3")
    testImplementation("io.mockk:mockk:1.12.3")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.6.1")
}

tasks.test { useJUnitPlatform() }

tasks.withType<KotlinCompile> { kotlinOptions.jvmTarget = "1.8" }

application { mainClass.set("MainKt") }

jib {
    from { image = "eclipse-temurin:17-jre-alpine" }
    to { image = "epelesis/calvinium" }
}

ktfmt { kotlinLangStyle() }
