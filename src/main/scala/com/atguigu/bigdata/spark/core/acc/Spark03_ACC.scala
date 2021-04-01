package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_ACC {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("ACC")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    //获取系统累加器
    //spark默认提供了简单的数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")
    val mapRDD = rdd.map(
      num => {
        //使用累加器
        sumAcc.add(num)
        num
      }
    )
    //获取累加器的值
    //累加器少加:转换算子中调用累加器,如果没有行动算子的话,就不会执行
    //一把情况下,累加器会放在行动算子中进行操作
    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)


    sc.stop()
  }
}
