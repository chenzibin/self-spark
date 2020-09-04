package self.spark.scala.demo

import org.apache.spark.sql.SparkSession

/**
  * SampleApp
  *
  * @author chenzb
  * @date 2020/8/11
  */
class SampleApp {

  def main(args: Array[String]): Unit = {
    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val spark = SparkSession.builder()
        .master("")
        .appName("Simple Application")
        .config("spark.executor.instances", "10")
        .config("spark.executor.memory", "10g")
        .getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }


}
