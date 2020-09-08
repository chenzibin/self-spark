package self.spark.scala.demo

import java.util.Random

import org.apache.spark.sql.SparkSession

/**
  * CoGroupTest
  *
  * @author chenzb
  * @date 2020/9/8
  */
object CoGroupTest {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .master("local")
            .appName("cogroup test")
            .getOrCreate()

        val numMappers = 3
        val numKVPairs = 4
        val valSize = 1000
        val numReducers = 2

        var input = 0 until numMappers

        val pairs = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
            val ranGen = new Random
            val arr1 = new Array[(Int, Int)](numKVPairs)
            for (i <- 0 until numKVPairs) {
                arr1(i) = (ranGen.nextInt(numKVPairs), ranGen.nextInt(1000))
            }
            arr1
        }

        val pairs1 = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
            val ranGen = new Random
            val arr1 = new Array[(Int, Int)](numKVPairs)
            for (i <- 0 until numKVPairs) {
                arr1(i) = (ranGen.nextInt(numKVPairs), ranGen.nextInt(1000))
            }
            arr1
        }.cache()

        val coGroupResult = pairs.cogroup(pairs1)
        coGroupResult.count()

        Thread.sleep(1000 * 60 * 30)
        spark.stop()
    }
}
