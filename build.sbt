organization := "com.despegar.p13n"

name := "kafka-example"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers ++= Seq(
    "Mariot Chauvin" at "http://mchv.me/repository",
    "Codahale" at "http://repo.codahale.com",
    "Typesafe" at "http://repo.typesafe.com",
    "SnowPlow" at "http://maven.snplow.com/releases",
    "Maven Central Repo" at "http://search.maven.org"
    )

libraryDependencies ++= List(
        "org.scalatest" %% "scalatest" % "2.2.4" % "test",
        "org.apache.kafka" %% "kafka" % "0.9.0.1" withSources() withJavadoc()
        )
        
EclipseKeys.withSource := true
