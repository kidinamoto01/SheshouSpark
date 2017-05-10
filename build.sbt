

name := "SheshouSpark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.0.2"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.10" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.0.0"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.12"

libraryDependencies +="org.apache.kafka"%"kafka-clients"%"0.10.0.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.26"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "2.1.0"

libraryDependencies += "org.elasticsearch" % "elasticsearch-spark-20_2.10" % "5.2.2"