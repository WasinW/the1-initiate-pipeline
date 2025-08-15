ThisBuild / scalaVersion := "2.12.19"

lazy val root = (project in file(".")).
  settings(
    name := "the1-initiate-pipeline",
    organization := "com.example",
    version := "0.1.0-SNAPSHOT",
    libraryDependencies ++= Seq(
      // Google Cloud BigQuery client
      "com.google.cloud" % "google-cloud-bigquery" % "2.37.0",
      // Google Cloud Storage Transfer Service client
      "com.google.cloud" % "google-cloud-storage-transfer" % "1.13.0",
      // AWS SDK for Glue (for schema)
      "software.amazon.awssdk" % "glue" % "2.25.36",
      // AWS SDK for S3 (optional, for reading sample files)
      "software.amazon.awssdk" % "s3" % "2.25.36",
      // Jackson YAML parser for config file
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.17.0",
      // Jackson core/databind
      "com.fasterxml.jackson.core" % "jackson-core" % "2.17.0",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.0",
      // Google Cloud Secret Manager client
      "com.google.cloud" % "google-cloud-secretmanager" % "2.20.0",
      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.8"
    )
  )

// Assembly plugin to build a fat JAR (optional)
// To enable, add sbt-assembly in project/plugins.sbt