import org.gradle.api.JavaVersion

plugins {
    java
    id("me.ragill.nar-plugin") version "1.0.2"
}

group = "org.apache.nifi.processors.swift"
version = "1.0.0"
description = "ListSwift Processor Example for NiFi (Java 8)"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

repositories {
    mavenCentral()
}

dependencies {

    implementation("org.javaswift:joss:0.10.4")

    implementation("org.apache.nifi:nifi-utils:1.26.0")
    implementation("org.apache.nifi:nifi-api:1.26.0")
    implementation("org.apache.nifi:nifi-distributed-cache-client-service-api:1.26.0")
    implementation("org.apache.nifi:nifi-listed-entity:1.26.0")
    implementation("org.apache.nifi:nifi-record-serialization-service-api:1.26.0")
    implementation("org.apache.nifi:nifi-record:1.26.0")
    implementation("org.apache.nifi:nifi-standard-record-utils:1.26.0")
    implementation("org.apache.nifi:nifi-ssl-context-service-api:1.26.0")
    implementation("org.apache.nifi:nifi-schema-registry-service-api:1.26.0")

    implementation("org.apache.jclouds:jclouds-core:2.5.0")
    implementation("org.apache.jclouds:jclouds-all:2.5.0")
    implementation("org.apache.jclouds.api:openstack-keystone:2.5.0")

    testImplementation("org.apache.nifi:nifi-mock:1.26.0")
    testImplementation("org.apache.nifi:nifi-mock-record-utils:1.26.0")
    testImplementation("org.apache.nifi:nifi-record-serialization-services:1.26.0")
    testImplementation("junit:junit:4.13.2")

    nar("org.apache.nifi:nifi-standard-services-api-nar:1.26.0")

}
