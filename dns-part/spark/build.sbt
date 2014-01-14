name := "dnsudf_spark"

version := "0.0"

scalaVersion := "2.9.3"

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= Seq(
  "com.google.protobuf" % "protobuf-java" % "2.3.0",
  "org.apache.hadoop" % "hadoop-core" % "1.1.2",
  "commons-lang" % "commons-lang" % "2.6",
  "org.specs2" %% "specs2" % "1.12.4.1" % "test",
  "org.spark-project" %% "spark-core" % "0.7.3"
	)

{
	  lazy val myHprofTask = TaskKey[Unit]("my-hprof-task")
	    seq(
		    fullRunTask(myHprofTask in Compile, Compile, "org.chris.dnsproc.IsWhitelisted"),
			    fork in myHprofTask := true,
				    javaOptions in myHprofTask += "-Xrunhprof:cpu=times,depth=8"
					  )
}


