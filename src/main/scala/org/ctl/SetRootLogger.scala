package org.ctl

// Need to either set root logger in code or manually specify log4j file for Spark.
// See Here for more details: https://stackoverflow.com/a/55596389/1000455
object SetRootLogger {
  import org.apache.log4j.{Level, Logger}

  def setLevel() = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  }
}
