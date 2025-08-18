ThisBuild / scalaVersion := "2.12.17"  // เปลี่ยนจาก 2.12.19

lazy val root = (project in file("."))
  .settings(
    name := "the1-initiate-pipeline",
    organization := "com.example",
    version := "1.0.0",
    
    libraryDependencies ++= Seq(
      // Google Cloud BigQuery client
      "com.google.cloud" % "google-cloud-bigquery" % "2.37.0",
      
      // Google Cloud Storage client
      "com.google.cloud" % "google-cloud-storage" % "2.29.1",
      
      // ⭐ อัพเดท Storage Transfer Service version
      "com.google.cloud" % "google-cloud-storage-transfer" % "1.5.0",  // จาก 1.42.0
      
      // Google Cloud Secret Manager client
      "com.google.cloud" % "google-cloud-secretmanager" % "2.28.0",
      
      // เพิ่ม dependencies ที่จําเป็น
      "com.google.protobuf" % "protobuf-java" % "3.25.1",
      "io.grpc" % "grpc-core" % "1.59.0",
      "io.grpc" % "grpc-protobuf" % "1.59.0",
      
      // Jackson for YAML and JSON parsing
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.17.0",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.17.0",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.17.0",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.0",
      
      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.8",
      "org.slf4j" % "slf4j-api" % "2.0.7",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "org.mockito" %% "mockito-scala" % "1.17.12" % Test
    ),
    
    // เพิ่ม resolvers
    resolvers ++= Seq(
      "Maven Central" at "https://repo1.maven.org/maven2/",
      "Google Maven" at "https://maven.google.com/"
    ),
    
    // Assembly plugin settings
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs match {
          case "MANIFEST.MF" :: Nil => MergeStrategy.discard
          case "services" :: _ => MergeStrategy.concat
          case _ => MergeStrategy.discard
        }
      case "reference.conf" => MergeStrategy.concat
      case x if x.endsWith(".proto") => MergeStrategy.first
      case _ => MergeStrategy.first
    },
    
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    
    // Java options สําหรับ compatibility
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-feature",
      "-unchecked",
      "-target:jvm-1.8",  // เพิ่ม JVM target
      "-language:postfixOps",
      "-language:implicitConversions"
    )
  )