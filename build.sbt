ThisBuild / name         := "quillspark-examples"
ThisBuild / organization := "com.github.ctl"
ThisBuild / scalaVersion := "2.12.10"
ThisBuild / version      := "0.0.1-SNAPSHOT"
ThisBuild / useCoursier  := true

val framelessVersion = "0.8.0"


lazy val `quill-spark-examples` = (project in file("."))
  .settings(
    resolvers ++= Seq(
      "jcenter" at "https://jcenter.bintray.com"
    ),
    scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 13)) =>
          Seq("-Ypatmat-exhaust-depth", "40")
        case Some((2, 11)) =>
          Seq("-Xlint",
            //"-Xfatal-warnings",
            "-Xfuture",
            "-deprecation",
            "-Yno-adapted-args",
            "-Ywarn-unused-import", "" +
              "-Xsource:2.12" // needed so existential types work correctly
          )
        case Some((2, 12)) =>
          Seq(
            "-Xlint:-unused,_",
            "-Xfuture",
            "-deprecation",
            "-Yno-adapted-args",
            "-Ywarn-unused:imports",
            "-Ycache-macro-class-loader:last-modified"
          )
        case _ => Seq()
      }
    },

    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.11.717",
      "io.getquill" %% "quill-jdbc" % "3.5.0",
      "io.getquill" %% "quill-spark" % "3.5.0",
      "org.apache.spark" %% "spark-core" % "2.4.4",
      "org.apache.spark" %% "spark-streaming" % "2.4.4",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4",
      "org.apache.spark" %% "spark-hive" % "2.4.4",
      "org.apache.spark" %% "spark-avro" % "2.4.4",
      "net.andreinc.mockneat" % "mockneat" % "0.3.7",

      "org.typelevel" %% "frameless-dataset" % framelessVersion,
      "org.typelevel" %% "frameless-ml"      % framelessVersion,
      "org.typelevel" %% "frameless-cats"    % framelessVersion,

      "org.typelevel" %% "spire" % "0.14.1"
    )
  )
