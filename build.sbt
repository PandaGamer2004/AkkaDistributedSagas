ThisBuild / resolvers += "Akka library repository".at("https://repo.akka.io/maven")

lazy val akkaVersion = "2.9.3"

lazy val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-actor-typed"         % akkaVersion,
  "ch.qos.logback"     % "logback-classic"          % "1.2.13",
  "io.getquill"       %% "quill-jdbc"               % "3.18.0",
  "org.postgresql"     % "postgresql"               % "42.2.8",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest"     %% "scalatest"                % "3.2.15"    % Test,
  "org.scalameta"     %% "munit"                    % "0.7.29"    % Test
)

lazy val core = (project in file("."))
  .settings(
    version := "1.0",
    scalaVersion := "3.4.0",
    name := "sample-akka",
    fork := true,
    libraryDependencies ++= coreDependencies
  )
