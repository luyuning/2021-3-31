package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ACC")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )
    println("sum=" + sum)
    sc.stop()

  }
}
