import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.10"
}

dependencies {
    api("com.google.code.gson:gson:2.9.0")

    api("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.0")

    api("org.apache.kafka:kafka-clients:3.1.0")
    testImplementation(kotlin("test"))
}

repositories {
    mavenCentral()
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "17"
    }

    withType<Test> {
        useJUnitPlatform()
    }
}

kotlin.sourceSets["main"].kotlin.srcDirs("code")
kotlin.sourceSets["test"].kotlin.srcDirs("test")