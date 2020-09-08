/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import java.util.Random

import org.apache.spark.sql.SparkSession

/**
  * Usage: GroupByTest [numMappers] [numKVPairs] [KeySize] [numReducers]
  */
object GroupByTest {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("GroupBy Test")
            .master("local")
            .getOrCreate()

        val numMappers = 3
        val numKVPairs = 4
        val valSize = 1000
        val numReducers = 2

        var input = 0 until numMappers

        val pairs1 = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
            val ranGen = new Random
            val arr1 = new Array[(Int, Int)](numKVPairs)
            for (i <- 0 until numKVPairs) {
                arr1(i) = (ranGen.nextInt(numKVPairs), ranGen.nextInt(1000))
            }
            arr1
        }.cache()
        // Enforce that everything has been calculated and in cache
        println(pairs1.count())
        println(pairs1.toDebugString)

        val result = pairs1.groupByKey(numReducers)
        println(result.count())
        println(result.toDebugString)

        val reduceAvgResult = pairs1.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        println(reduceAvgResult.collect())
        println(reduceAvgResult.toDebugString)

        val aggregateResult = pairs1.aggregateByKey("0")(_ + "_" + _ , _ + "@" + _)
        aggregateResult.foreach(println)

        val combineResult = pairs1.combineByKey(
            (c:Int) => c,
            (_:Int) + (_:Int),
            (c:Int, c2:Int) => c + c2,
            2
        )

        val foldResult = pairs1.foldByKey(0)(_+_)

        Thread.sleep(1000 * 60 * 30)
        spark.stop()
    }
}

// scalastyle:on println
