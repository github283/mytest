package com.yuce

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shkstart 
  * @create 2020-11-22 20:16 
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val value: RDD[String] = sc.textFile("C:\\companyFile\\spark3-stu\\datas\\word")
    val word1: RDD[String] = value.flatMap(_.split(" "))
    val word2: RDD[(String, Int)] = word1.map((_,1))
    val word3: RDD[(String, Int)] = word2.reduceByKey(_ + _)
    val tuples: Array[(String, Int)] = word3.collect()
    //test
    tuples.foreach(println)
    sc.stop()
  }
}
