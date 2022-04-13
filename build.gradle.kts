import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.20"
    application
    id("com.google.cloud.tools.jib") version "2.3.0"
}

group = "com.nedellis"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val KOTEST_VERSION = "5.2.2"

dependencies {
    implementation("com.google.guava:guava:31.1-jre")
    implementation("org.rocksdb:rocksdbjni:7.0.4")

    testImplementation(kotlin("test"))
    testImplementation("io.kotest:kotest-runner-junit5:$KOTEST_VERSION")
    testImplementation("io.kotest:kotest-assertions-core:$KOTEST_VERSION")
    testImplementation("io.mockk:mockk:1.12.3")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

application {
    mainClass.set("MainKt")
}

jib {
    from {
        image = "eclipse-temurin:17-jre-alpine"
    }
    to {
        image = "epelesis/calvinium"
    }
}