import com.google.protobuf.gradle.id

plugins {
  java
  application
  id("com.google.protobuf") version "0.9.1"
}

dependencies {
  repositories {
    mavenLocal()
    mavenCentral()
  }

  // Restate SDK
  implementation("dev.restate.sdk:sdk-java-blocking:1.0-SNAPSHOT")
  implementation("dev.restate.sdk:sdk-http-vertx:1.0-SNAPSHOT")
  implementation("dev.restate.sdk:sdk-lambda:1.0-SNAPSHOT")
  // To use Jackson to read/write state entries (optional)
  implementation("dev.restate.sdk:sdk-serde-jackson:1.0-SNAPSHOT")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.2")

  // Protobuf and grpc dependencies
  implementation("com.google.protobuf:protobuf-java:3.24.3")
  implementation("io.grpc:grpc-stub:1.58.0")
  implementation("io.grpc:grpc-protobuf:1.58.0")
  // This is needed to compile the @Generated annotation forced by the grpc compiler
  // See https://github.com/grpc/grpc-java/issues/9153
  compileOnly("org.apache.tomcat:annotations-api:6.0.53")

  // Logging (optional)
  implementation("org.apache.logging.log4j:log4j-core:2.20.0")
}

// Configure protoc plugin
protobuf {
  protoc { artifact = "com.google.protobuf:protoc:3.24.3" }

  plugins {
    id("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:1.58.0" }
  }

  generateProtoTasks {
    all().forEach {
      it.plugins {
        id("grpc")
      }
    }
  }
}

application {
  mainClass.set("dev.restate.sdk.examples.Greeter")
}

// Temporary solution for disabling caching of Java SDK until we release it
configurations.all {
  // This disables caching for -SNAPSHOT dependencies
  resolutionStrategy.cacheChangingModulesFor(0, "seconds")
}
