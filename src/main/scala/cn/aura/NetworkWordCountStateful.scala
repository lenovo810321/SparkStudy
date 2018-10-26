package cn.aura

import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 张宝玉 on 2018/10/26.
  */
object NetworkWordCountStateful {
  def main(args: Array[String]): Unit = {
    //定义状态更新函数
    val updataFunc = (values : Seq[Int], state : Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val conf = new SparkConf().setAppName("NetworkWordCountStateful").setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(5))
    sc.checkpoint("E:\\myproject\\data\\checkpoint") //设置检查点，容错
    val lines = sc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(word => (word,1))
    val stateDStream = wordDstream.updateStateByKey[Int](updataFunc)
    stateDStream.print()
    sc.start()
    sc.awaitTermination()
  }
}
