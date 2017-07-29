package scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ang on 2017/7/12.
  */
class AngTran {

}

object Ang{
  def main(args: Array[String]) {

    val logFile = "F:/dsj/data/input.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
    var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)

    rdd1.join(rdd2).foreach(x=>println(x))
  }


}