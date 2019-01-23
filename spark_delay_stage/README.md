Modify the following files of the open source version spark-2.3.1 to implement simple delay stage scheduling:

1. In spark-2.3.1/core/src/main/scala/org/apache/spark/metrics/MetricsConfig.scala, a function getStageId() has been added to read the value of "stageId" in the configuration file, which specifies the ID of the critical stage, and a function getStageDelayTime() has been added to read the value of “stageDelayTime” in the configuration file, which specifies the delaying time of stage: 

/**
 * Get the StageId
  */
def getStageId(): Properties = {
    setDefaultProperties(properties)
    loadPropertiesFromFile(conf.getOption("spark.metrics.conf"))
    val prefix = "spark.metrics.conf."
    conf.getAll.foreach {
      case(k, v) if k.startsWith(prefix) =>
        properties.setProperty(k.substring(prefix.length()), v)
      case _ =>
    }
    properties.setProperty("stageId", properties.getProperty("stageId"))
    properties
  }

  /**
    * Get the Delay time of Stage
    */
  def getStageDelayTime(): Properties = {
    setDefaultProperties(properties)
    loadPropertiesFromFile(conf.getOption("spark.metrics.conf"))
    val prefix = "spark.metrics.conf."
    conf.getAll.foreach {
      case(k, v) if k.startsWith(prefix) =>
        properties.setProperty(k.substring(prefix.length()), v)
      case _ =>
    }
    properties.setProperty("stageDelayTime", properties.getProperty("stageDelayTime"))
    properties
  }


2. In spark-2.3.1/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala, modify function submitStage():

/** Submits stage, but first recursively submits any missing parents. */
private def submitStage(stage: Stage) {
    val sparkConf = new SparkConf(loadDefaults = false)
    sparkConf.set("spark.metrics.conf", "/usr/local/spark/conf/metrics.properties")
    val conf = new MetricsConfig(sparkConf)
    val para1 = conf.getStageId().getProperty("stageId")
    val para2 = conf.getStageDelayTime().getProperty("stageDelayTime")
    val A = new ArrayBuffer[Int]()
    val B = new ArrayBuffer[Int]()
    for(a1 <- para1.split(",")) {
        A += a1.toInt
    }
    for(b1 <- para2.split(",")) {
        B += b1.toInt
    }
    for(i <- 0 until A.length) {
        if (stage.id == A(i)) {
            Thread.sleep (B(i))
        }
    }
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
        logDebug("submitStage(" + stage + ")")
        if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
            val missing = getMissingParentStages(stage).sortBy(_.id)
            logDebug("missing: " + missing)
            if (missing.isEmpty) {
                logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
                submitMissingTasks(stage, jobId.get)
            } else {
                for (parent <- missing) {
                    submitStage(parent)
                }
                waitingStages += stage
            }
        }
    } else {
        abortStage(stage, "No active job for stage " + stage.id, None)
    }
}

3. Using maven to compile the modified version of the spark source code into a binary installation package file.
