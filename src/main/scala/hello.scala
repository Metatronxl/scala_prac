import java.io._

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.log4j.{Level, Logger}

import scala.io.StdIn
//import org.apache.spark.{SparkConf, SparkContext}
import Array._
import scala.io._




object hello{
  //设置日志级别
  Logger.getLogger("org").setLevel(Level.WARN)

  /***
    * 1.新建SparkConf的配置
    * 2.了解kryo序列化，注意kryo对自定义类需要注册
    * example：
    *       import org.apache.spark.SparkConf
    *       val conf: SparkConf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
    *                                             .setMaster("local").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    *                                             .registerKryoClasses(Array[Class[_]](classOf[Nothing]))
    * 3.生成tuple(,)来计算文本单词个数
    * 4. term._1,term._2指定tuple里的第一个/第二个元素
    * 5.使用map来更换(key,value)，sortByKey（false降序，true升序）来对数量大小进行排序
    *
    */
  def test1(): Unit ={
    val sparkConf = new SparkConf().setAppName("AttrProcess").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")//set("spark.executor.memory", "80g")//setMaster("local")//
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("README.md")
    val words = lines.flatMap(line => line.split(" "))
    val counts = words.map(word =>(word,1)).reduceByKey((x,y) => x+y)
    val sortCount = counts.map(term=>(term._2,term._1)).sortByKey(false).take(10)
    for(temp <- sortCount) println(temp)


  }

  def main(args: Array[String]) {

   println(test1())
  }





}