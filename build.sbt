ThisBuild / resolvers += "Akka library repository".at("https://repo.akka.io/maven")

lazy val akkaVersion = "2.9.3"

lazy val coreDependencies = Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.13",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

lazy val printGitIgnore = taskKey[Unit]("A simple hello task")




lazy val core = (project in file("."))
  .settings(
    printGitIgnore := {
      val log = streams.value.log
      val gitIgnoreFile = baseDirectory.value / ".gitignore"

      if(gitIgnoreFile.exists()){
        val fileContent = IO.read(gitIgnoreFile)
        log.info(s"Content of file ${gitIgnoreFile.name} ${fileContent}")
      }

    },
    version := "1.0",
    scalaVersion := "3.4.0",
    name := "sample-akka",
    fork := true,
    libraryDependencies ++= coreDependencies
  )

