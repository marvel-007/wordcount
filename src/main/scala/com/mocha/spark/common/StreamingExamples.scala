package com.mocha.spark.common

import org.apache.log4j.{Level, Logger}

/**
  * @author Yangxq
  * @version 2017/5/15 14:23
  */
object StreamingExamples {
  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
