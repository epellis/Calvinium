import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.google.protobuf.gradle.*

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath("com.google.protobuf:protobuf-gradle-plugin:0.8.18")
    }
}

plugins {
    kotlin("jvm") version "1.6.20"
    application
    id("com.google.cloud.tools.jib") version "2.3.0"
    id("com.ncorti.ktfmt.gradle") version "0.8.0"
    id("java")
    id("com.google.protobuf") version "0.8.18"
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        jvmTarget = "11"
        kotlinOptions.freeCompilerArgs += "-opt-in=kotlin.RequiresOptIn"
    }
}

sourceSets {
    main {
        java {
            srcDir("build/generated/source/proto/main/java")
            srcDir("build/generated/source/proto/main/kotlin")
            srcDir("build/generated/source/proto/main/grpc")
            srcDir("build/generated/source/proto/main/grpckt")
        }
    }
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
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-guava:1.6.1")

    runtimeOnly("io.grpc:grpc-netty-shaded:1.45.1")
    implementation("io.grpc:grpc-protobuf:1.45.1")
    implementation("io.grpc:grpc-stub:1.45.1")
    implementation("io.grpc:grpc-kotlin-stub:1.2.1")
    implementation("com.google.protobuf:protobuf-kotlin:3.20.1")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53") // necessary for Java 9+

    implementation("com.linecorp.armeria:armeria:1.16.0")
    implementation("com.linecorp.armeria:armeria-grpc:1.16.0")
    implementation("com.linecorp.armeria:armeria-kotlin:1.16.0")
    implementation("com.linecorp.armeria:armeria-protobuf:1.16.0")

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

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.20.1"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.45.1"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:1.2.1:jdk7@jar"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without options.
                id("grpc")
                id("grpckt")
            }
            it.builtins {
                id("kotlin")
            }
        }
    }
}
