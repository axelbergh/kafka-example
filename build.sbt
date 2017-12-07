organization := "com.despegar.p13n"

name := "kafka-example"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.2"

resolvers ++= Seq(
    "Mariot Chauvin" at "http://mchv.me/repository",
    "Codahale" at "http://repo.codahale.com",
    "Typesafe" at "http://repo.typesafe.com",
    "SnowPlow" at "http://maven.snplow.com/releases",
    "Maven Central Repo" at "http://search.maven.org"
    )

libraryDependencies ++= List(
        "org.apache.kafka" % "kafka_2.11" % "0.10.1.0" withSources() withJavadoc()
        )
        
EclipseKeys.withSource := true
