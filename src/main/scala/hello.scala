import java.io._

import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark

import scala.io.StdIn
//import org.apache.spark.{SparkConf, SparkContext}
import Array._
import scala.io._




object hello{
  //设置日志级别
  Logger.getLogger("org").setLevel(Level.WARN)
  val sparkConf = new SparkConf().setAppName("AttrProcess").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")//set("spark.executor.memory", "80g")//setMaster("local")//
  val sc = new SparkContext(sparkConf)
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
    * 6. for循环打印
    * 7. saveAsTextFile会生成RDD文件夹
    */
  def start(): Unit ={
//    val sparkConf = new SparkConf().setAppName("AttrProcess").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")//set("spark.executor.memory", "80g")//setMaster("local")//
//    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("README.md")
    val words = lines.flatMap(line => line.split(" "))
    val counts = words.map(word =>(word,1)).reduceByKey((x,y) => x+y)
    val sortCount = counts.map(term=>(term._2,term._1)).sortByKey(false).take(10)
    for(temp <- sortCount) println(temp)
    // sortCount.saveAsTextFile('test2')

  }

  def RDDTest(): Unit ={
//    val sparkConf = new SparkConf().setAppName("AttrProcess").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local")//set("spark.executor.memory", "80g")//setMaster("local")//
//    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("README.md")

//    val error = lines.filter(line => line.contains("error"))
//    println(error)
//
    val nums = sc.parallelize(List(1,2,3,4))
    val result = nums.map(x => x*x)

    println(result.collect().mkString(","))



  }

  /**
    * aggregate(（），（）)函数
    * 第一个参数是我们期待返回的类型的初始值
    * 第二个参数：通过函数吧RDD中的元素合并起来放入累加器
    * 第三个参数：将第二个参数中的累加器两两合并
    *
    * result：是list的平均值
    *
    */
  def aggreGate(): Unit ={

    val num = sc.parallelize(List(2,3,4,5,6,7,8,9,10))
    val sumCount = num.aggregate((0,0))(
      (acc,value) => (acc._1+value,acc._2+1),
      (acc1,acc2) => (acc1._1+acc2._1, acc1._2+acc2._2)
    )
  }

  /**
    * map函数可以生成PairRDD
    * ex： rdd.map(x =>(x.split(",")(0),x))
    * 或者用sc.parallelize(list((1,2),(1,3),(2,3)))
    *
    *
    * groundByKey()对具有相同键的值进行分组
    *
    * fold与reduce类似，不过多了一个初始值rdd.fold(value)(func)
    *
    * partitionBy(new spark.HashPartitioner()) 进行数据hash分区，
    *   这样做的好处就在于，分区过的Rdd在进行join（）等操作时，不会再把hash
    *   过的RDD再次数据混洗，而是只对另一个RDD进行混洗操作（也就是说RDD.partitionBy().persisit()
    *   对于数据量大的RDD来说，可以有效提升Spark的性能）
    *
    *   数据分区对于提升spark效率有很大的帮助
    *   cogroup（）、groupWith（）、join（）、leftOuterJoin（）、rightOutJoin（）、groupByKey（）、reduceByKey（）、
    *   combineByKey（）、lookup（）
    *   都会从数据分区中获益
    */
  def PairRDDTest(): Unit ={
      val rdd = sc.parallelize(List("1,24.33.23","2,234.43.23","1,23.342.43","3,12.32.452"))
//      val rdd = sc.parallelize(List((1,"23.3.2.3"),(2,"323.23.42"),(2,"23.231.23")))
      val words = rdd.map(x =>(x.split(",")(0),x))
//      val words = rdd.map(x =>(x.split))
      val newWords = words.groupByKey()
      newWords.collect().foreach(println)

      //对words进行分区
      val partitioned = words.partitionBy(new spark.HashPartitioner(2))
      println(partitioned.partitioner) // result is 2

  }

  def main(args: Array[String]) {

//   println(start())
//     RDDTest()
    PairRDDTest()
  }





}