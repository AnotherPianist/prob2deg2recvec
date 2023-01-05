name := "Prob2Deg2RecVec"

version := "1.0"

scalaVersion := "2.12.15"

javacOptions ++= Seq("-source", "1.8", "-g:none")

scalacOptions ++= Seq("-optimise", "-optimize","-target:jvm-1.8")

libraryDependencies += "it.unimi.dsi" % "fastutil" % "8.5.9"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.2"