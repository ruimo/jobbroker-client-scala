name := "jobbroker-client-scala"
organization := "com.ruimo"
scalaVersion := "2.12.6"

publishTo := Some(
  Resolver.file(
    "jobbroker-client-scala",
    new File(Option(System.getenv("RELEASE_DIR")).getOrElse("/tmp"))
  )
)

resolvers += "ruimo.com" at "http://static.ruimo.com/release"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies += "com.ruimo" %% "jobbroker-dao" % "1.0"
libraryDependencies += "com.ruimo" %% "jobbroker-queue" % "1.0"
libraryDependencies += "com.ruimo" %% "scoins" % "1.17"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"

libraryDependencies += "org.specs2" %% "specs2-core" % "4.3.3" % Test
libraryDependencies += "com.h2database"  %  "h2" % "1.4.197" % Test
libraryDependencies += "com.github.fridujo" % "rabbitmq-mock" % "1.0.3" % Test
