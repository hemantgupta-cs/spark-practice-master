
name := "spark-practice"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1"


//
//resolvers ++= Seq(
//  "apache-snapshots" at "http://repository.apache.org/snapshots/"
//)
//
//libraryDependencies ++= Seq (
//  "org.apache.spark" %% "spark-core" % "2.1.0"
//  ,"org.apache.spark" %% "spark-sql" % "2.1.0"
//  ,"org.apache.spark" %% "spark-mllib" % sparkVersion
//  ,"org.apache.spark" %% "spark-streaming" % sparkVersion
//   ,"org.apache.spark" %% "spark-hive" % "2.1.0"
//)
/* joda time*/
//libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.16.0"