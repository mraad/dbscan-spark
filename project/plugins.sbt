resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")

logLevel := Level.Warn
