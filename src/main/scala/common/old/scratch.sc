import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID
import java.time.Instant
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD
val (cores, partitions) = ("5", 1)

val spark: SparkSession = SparkSession
  .builder()
  .appName("test")
  .master(s"local[$cores]")
  .getOrCreate()
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import spark.implicits._
val animalsRdd: RDD[Row] = {
  (1 to 23).map(i => s"dog-${i}").toList.toDF("animalType").rdd
}

//val animalsRdd: RDD[Row] = {
//  (1 to 23).map(i => s"dog-${i}").toList.toDF("animalType").rdd
//}
//MapPartitionsRDD[4]
val animalsRddPS = animalsRdd.partitions.size

val data = Array(1, 2, 3, 4, 5)
val distData = spark.sparkContext.parallelize(data, 2)
val distDataPS =distData.partitions.size

//animalsRdd.collect().foreach(println)
//val strRow = animalsRdd.coalesce(2).map(row => row + row.get(0).toString)
val strRow = animalsRdd.repartition(1).map(row => row + row.get(0).toString)
//MapPartitionsRDD[5]
strRow.partitions.size
//MapPartitionsRDD[9] with repartition
strRow.collect().foreach(println)
//rowL.collect().foreach(println)
//animalsRdd.repartition(partitions).map { row =>
//  println(s"THE row is $row / and first element is ${row.get(0).toString}")
//  val animal = row.get(0).toString
//  println(s"animal: $animal")
//}
//  private def calculateSumOfScores(partitions: Int, animalsRdd: RDD[Row]) = {
//    val results: RDD[fixed.AnimalTestResult] = animalsRdd.repartition(partitions).map { row =>
//      println(s"THE row is $row / and first element is ${row.get(0).toString}")
//      val animal = row.get(0).toString
//      val sessionId = AnimalWeighingService.establishSession
//      fixed.AnimalTestResult(animal, AnimalWeighingService.testSubjectAndReportScore(sessionId, animal))
//    }
//    results.collect().map(_.score).sum
//  }