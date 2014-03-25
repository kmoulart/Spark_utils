package kmoulart.bigdata.spark_utils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object TestLauncher {

  def main(args: Array[String]) {
    testClassifers(args(0), ',', 100, false)
  }

  /**
   * @param fileName the file containing the training data
   * @param sep the separator used in the csv file
   * @param nbCol number of columns to take into account
   * @param nonStd tells if the labels in the file are non standards to change them on the fly to 0 and 1
   */
  def testClassifers(fileName: String, sep: Char, nbCol: Int, nonStd: Boolean) = {
    // Load and parse the data file
    val conf = new SparkConf()
    		.setMaster("node01.bigdata")
			.setAppName("Test application")
			.set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)
    val data = sc.textFile(fileName).cache
    val parsedData = parseToLabeledPoints(sep, nbCol, nonStd, data)

    testLogisticRegression(parsedData)
    
    testNaiveBayes(parsedData)
    
    val numIterations = 200
    testSVM(parsedData, numIterations)
  }
  
  private def lineFilterTest: Unit = {
    val logFile = "/home/cacf/Workspace_eclipse/spark_project/src/test/scala/samples/junit.scala" // Should be some file on your system
    val sc = new SparkContext("local", "Simple App", "/opt/cloudera/parcels/SPARK/lib/spark/",      List("target/spark_project-0.0.1-SNAPSHOT.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
  
  private def testLogisticRegression(parsedData: RDD[LabeledPoint]): Unit = {
    val model = new LogisticRegressionWithSGD().run(parsedData)
    val trainErr = evaluateModel(parsedData, model)
    println("Training Error = " + trainErr)
  }
  
  private def testNaiveBayes(parsedData: RDD[LabeledPoint]): Unit = {
    val model = new NaiveBayes().setLambda(0.5).run(parsedData)
    val trainErr = evaluateModel(parsedData, model)
    println("Training Error NB = " + trainErr)
  }
  
  private def testSVM(parsedData: RDD[LabeledPoint], numIterations: Int): Unit = {
    val model = SVMWithSGD.train(parsedData, numIterations)
    val trainErr = evaluateModel(parsedData, model)
    println("Training Error SVM = " + trainErr)
  }
  
  private def evaluateModel(parsedData: RDD[LabeledPoint], model: ClassificationModel): Double = {
    val labelAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
    trainErr
  }
  
  private def parseToLabeledPoints(sep: Char, nbCol: Int, nonStd: Boolean, data: RDD[String]): RDD[LabeledPoint] = {
    val parsedData = data.map { line =>
      val parts = line.split(sep).slice(0, nbCol)
      var label = parts(0).toDouble
      // If the label is not standard (0 and 1), it means it's 1 and 2, so we store it as standard to be processed
      if (nonStd)
        label = 0
      if (parts(0).toDouble > 1)
        label = 1
      LabeledPoint(label, parts.tail.map(x => x.toDouble).toArray)
    }
    parsedData
  }
}

