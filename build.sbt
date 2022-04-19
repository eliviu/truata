name := "truata"

version := "0.1"

scalaVersion := "2.12.13"
val sparkVersion = "3.1.2"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

Compile / unmanagedSourceDirectories += baseDirectory.value / "src"