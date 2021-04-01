package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

//广播变量
object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("ACC")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    rdd1.map {
      case (w, c) => {
        map.getOrElse(w, 0L)
        (w, (c, 1))
      }
    }.collect().foreach(println)
    sc.stop()

  }

  //    val rdd2 = sc.makeRDD(List(
  //      ("a", 4), ("b", 5), ("c", 6)
  //    ))
  //    //join会导致数据量几何增长,并且会影响shuffle的性能,不推荐使用
  //    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
  //    joinRDD.collect().foreach(println)//(a,(1,4)),(b,(2,5)),(c,(3,6))


}
