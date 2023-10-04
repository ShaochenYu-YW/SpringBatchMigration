import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

buildscript {
    dependencies {
        constraints {
            classpath("org.apache.logging.log4j:log4j-core") {
                version {
                    strictly("[2.17, 3[")
                    prefer("2.17.1")
                }
                because("CVE-2021-44228, CVE-2021-45046, CVE-2021-45105: Log4j vulnerable to remote code execution and other critical security vulnerabilities")
            }
        }
    }
}

plugins {
    id("org.springframework.boot") version "3.0.2"
    id("io.spring.dependency-management") version "1.1.0"
    kotlin("jvm") version "1.7.22"
    kotlin("plugin.spring") version "1.7.22"
}

group = "com.labelbox.adv.retrofill"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_17

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("mysql:mysql-connector-java:8.0.26")

    implementation("com.google.cloud:google-cloud-core:2.11.0")
    implementation("org.springframework.batch:spring-batch-integration")
    implementation("com.h2database:h2:1.4.200")
    implementation("org.springframework.boot:spring-boot-starter-batch")
    implementation("com.google.cloud:google-cloud-spanner:6.38.2")
    implementation("com.mysql:mysql-connector-j:8.0.32")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-batch")
    implementation("org.springframework.batch:spring-batch-integration")
    implementation("com.slack.api:bolt:1.29.1")
    implementation("com.slack.api:bolt-servlet:1.29.1")
    implementation("com.slack.api:bolt-jetty:1.29.1")
    implementation("org.slf4j:slf4j-simple:1.7.36")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.json:json:20201115")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("co.elastic.clients:elasticsearch-java:8.7.1")
    implementation("org.apache.commons:commons-dbcp2:2.9.0")


    // uncomment the below line to run locally without docker
    // providedRuntime("org.springframework.boot:spring-boot-starter-tomcat")
    developmentOnly("org.springframework.boot:spring-boot-devtools")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.batch:spring-batch-test")
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "17"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
