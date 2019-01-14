name := "sbt-application"

organization := "Semantive"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val sparkV = "2.1.0"

  Seq(
    "org.apache.spark" %% "spark-core" % sparkV % "provided",
    "org.apache.spark" %% "spark-sql" % sparkV % "provided",
    "org.apache.spark" % "spark-streaming_2.11" % sparkV,
    "org.apache.bahir" %% "spark-streaming-twitter" % sparkV
  )
}

//--------------------------------
//---- sbt-assembly settings -----
//--------------------------------

val mainClassString = "TwitterPopularTags"

mainClass in assembly := Some(mainClassString)

assemblyJarName := "spark-app.jar"

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

assemblyOption in assembly ~= { _.copy(cacheOutput = false) }

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { c =>
    c.data.getName.startsWith("log4j")
    c.data.getName.startsWith("slf4j-") ||
    c.data.getName.startsWith("scala-library")
  }
}

// Disable tests (they require Spark)
test in assembly := {}

// publish to artifacts directory
publishArtifact in(Compile, packageDoc) := false

publishTo := Some(Resolver.file("file", new File("artifacts")))

cleanFiles += baseDirectory { base => base / "artifacts" }.value

//--------------------------------
//----- sbt-docker settings ------
//--------------------------------
enablePlugins(sbtdocker.DockerPlugin)

dockerfile in docker := {
  val baseDir = baseDirectory.value
  val artifact: File = assembly.value

  val sparkHome = "/home/spark"
  val imageAppBaseDir = "/app"
  val artifactTargetPath = s"$imageAppBaseDir/${artifact.name}"

  val dockerResourcesDir = baseDir / "docker-resources"
  val dockerResourcesTargetPath = s"$imageAppBaseDir/"

  new Dockerfile {
    from("semantive/spark")
    maintainer("Semantive")
    env("APP_BASE", s"$imageAppBaseDir")
    env("APP_CLASS", mainClassString)
    env("SPARK_HOME", sparkHome)
    copy(artifact, artifactTargetPath)
    copy(dockerResourcesDir, dockerResourcesTargetPath)
    //Symlink the service jar to a non version specific name
    entryPoint(s"${dockerResourcesTargetPath}docker-entrypoint.sh")
  }
}
buildOptions in docker := BuildOptions(cache = false)

imageNames in docker := Seq(
  ImageName(
    namespace = Some(organization.value.toLowerCase),
    repository = name.value,
    // We parse the IMAGE_TAG env var which allows us to override the tag at build time
    tag = Some(sys.props.getOrElse("IMAGE_TAG", default = version.value))
  )
)
