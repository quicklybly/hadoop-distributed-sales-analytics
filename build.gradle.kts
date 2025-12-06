plugins {
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val hadoopVersion = "3.2.1"

dependencies {
    compileOnly("org.apache.hadoop:hadoop-common:$hadoopVersion")
    compileOnly("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion")

    // implementation("org.apache.commons:commons-csv:1.10.0")
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    shadowJar {
        // result file name 'sales-analytics.jar'
        archiveBaseName.set("sales-analytics")
        archiveClassifier.set("")
        archiveVersion.set("")

        // copy jar to jobs directory
        doLast {
            copy {
                from(archiveFile)
                into(rootProject.file("jobs"))
            }
            logger.quiet("JAR copied to jobs/ directory successfully!")
        }
    }

    wrapper {
        gradleVersion = "8.14.10"
        distributionType = Wrapper.DistributionType.BIN
    }
}
