ThisBuild / scalaVersion := "3.9.0"
ThisBuild / organization := "io.github.nmarasoiu"

// -Werror makes a non-exhaustive match an error, so a TigerBeetle result code
// that no rule handles fails the build. Strict equality makes `==` between
// unrelated types, such as an offset and a raw number, an error too.
ThisBuild / scalacOptions ++= Seq(
  "-deprecation", "-feature", "-unchecked", "-Wunused:all", "-Werror", "-language:strictEquality",
)

lazy val root = (project in file("."))
  .aggregate(core, bench)
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

// JMH benchmarks of the core: throughput and, with `-prof gc`, bytes
// allocated per operation. Run with `bench/Jmh/run -prof gc`.
lazy val bench = (project in file("bench"))
  .dependsOn(core)
  .enablePlugins(JmhPlugin)
  .settings(publish / skip := true)
