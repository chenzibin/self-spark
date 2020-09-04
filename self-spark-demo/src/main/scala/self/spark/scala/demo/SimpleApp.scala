package self.spark.scala.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SimpleAPp
  *
  * @author chenzb
  * @date 2020/8/11
  */
object SimpleApp {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setMaster("local")
        conf.setAppName("test")
        val sc = new SparkContext(conf)

        val rdd1 = sc.parallelize(List[(String, Int)](
            ("a", 100),
            ("b", 200),
            ("a", 100),
            ("c", 100)
        ), 3)
        val result:RDD[(String, Iterable[Int])] = rdd1.groupByKey()
        result.foreach(println)

    }

}
