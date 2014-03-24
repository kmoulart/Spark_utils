package cacf.bigdata.spark_project
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object App {
  def main(args: Array[String]) {
    val logFile = "/home/cacf/Workspace_eclipse/spark_project/src/test/scala/samples/junit.scala" // Should be some file on your system
    val sc = new SparkContext("local", "Simple App", "/opt/cloudera/parcels/SPARK/lib/spark/",
      List("target/spark_project-0.0.1-SNAPSHOT.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}