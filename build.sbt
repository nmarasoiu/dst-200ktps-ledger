ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "io.github.nmarasoiu"

// -Werror makes a non-exhaustive match an error, so a TigerBeetle result code
// that no rule handles fails the build. Strict equality makes `==` between
// unrelated types, such as an offset and a raw number, an error too.
ThisBuild / scalacOptions ++= Seq(
  "-deprecation", "-feature", "-unchecked", "-Wunused:all", "-Werror", "-language:strictEquality",
)

lazy val root = (project in file("."))
  .aggregate(core, sim, shell, contractTests, bench)
  .settings(publish / skip := true)

// The pure core (README section 11). It has no library dependencies outside
// tests, so no effect library is on its classpath.
lazy val core = (project in file("core"))
  .settings(
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.3.6" % Test,
      "org.scalameta" %% "munit-scalacheck" % "1.3.1" % Test,
    ),
  )

// The deterministic simulator (README section 11): the core under one seeded event loop, in one thread. It
// reuses the core's test fixtures, and like the core it has no effect library on its classpath.
lazy val sim = (project in file("sim"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.3.6" % Test,
      "org.scalameta" %% "munit-scalacheck" % "1.3.1" % Test,
    ),
  )

// The Cats Effect shell (README section 10). For now it holds the real TigerBeetle adapter (README section 14,
// stage 4). The client's version is the version of the server the contract suites run against.
lazy val shell = (project in file("shell"))
  .dependsOn(core)
  .settings(
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.7.1",
      "com.tigerbeetle" % "tigerbeetle-java" % "0.17.9",
    ),
  )

// One suite per port, run against the real system and against the simulated adapter (README section 14, stage 4).
// Forked, because a native client library loads once per JVM and Testcontainers needs the test's own class path.
lazy val contractTests = (project in file("contract-tests"))
  .dependsOn(shell, sim)
  .settings(
    publish / skip := true,
    Test / fork := true,
    Test / javaOptions += "--enable-native-access=ALL-UNNAMED",
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.3.6" % Test,
      "org.typelevel" %% "munit-cats-effect" % "2.2.1" % Test,
      "org.testcontainers" % "testcontainers" % "2.0.5" % Test,
      "org.slf4j" % "slf4j-nop" % "2.0.17" % Test,
    ),
  )

// JMH benchmarks of the core: throughput and, with `-prof gc`, bytes
// allocated per operation. Run with `bench/Jmh/run -prof gc`.
lazy val bench = (project in file("bench"))
  .dependsOn(core)
  .enablePlugins(JmhPlugin)
  .settings(publish / skip := true)
