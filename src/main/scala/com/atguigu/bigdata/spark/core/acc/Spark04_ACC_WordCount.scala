package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_ACC_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("ACC")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello", "spark", "hello"))
    //累加器:WordCount
    //创建累加器对象
    val wcACC = new MyAccumulator()
    //向Spark进行注册
    sc.register(wcACC, "wordcountAcc")
    rdd.foreach(
      word => {
        //数据的累加(使用累加器)
        wcACC.add(word)
      }
    )
    //获取累加器累加的结果
    println(wcACC.value)
    sc.stop()
  }

  //自定义累加器:WordCount
  //1.继承AccumulatorV,定义泛型
  //IN:累加器输入的数据类型String
  //ON:累加器返回的数据类型
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap = mutable.Map[String, Long]()

    //判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    //重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L)+1
      wcMap.update(word, newCnt)
    }

    //Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value
      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    //累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}
