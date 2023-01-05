package common.old

import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID
import java.time.Instant
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD
//import gradle.spark.quickstart.{AnimalWeighingService, SparkApp, fixed}

/**
 *
 * The implementation below currently fails. We are looking for the candidate to submit a
 * working solution to the currently buggy code with some explanation as below.
 * a. What is the issue.
 * b. What is your thought process while addressing the issue.
 * c. Some explanation on your solution.
 *
 */
case class AnimalTestResult(animal: String, score: Int)
/**
 * Uses predictive model to estimate average weight of any type of animal.
 */
object AnimalWeighingService extends Serializable {
  import java.util.concurrent.CopyOnWriteArrayList
  val activeSessions = new CopyOnWriteArrayList[String]()
  var nextScore = new AtomicInteger()
  def establishSession: String = {
    val session = UUID.randomUUID().toString
    activeSessions.add(session)
    Thread.sleep(100)
    val time = Instant.now.toEpochMilli
    System.out.println(Thread.currentThread().getName +   " returning session at:" + time);
    session
  }
  def testSubjectAndReportScore(sessionId: String, animal: String): Int = {
    if (!activeSessions.contains(sessionId)) {
      throw new IllegalArgumentException(s"Invalid session: $sessionId")
    }
    nextScore.incrementAndGet()
  }
}
object FixedSparkApp {
  def main(args: Array[String]): Unit = {
    val (cores, partitions) = ("1", 1)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("test")
      .master(s"local[$cores]")
      .getOrCreate()
    import spark.implicits._
    val animalsRdd: RDD[Row] = {
      (1 to 23).map(i => s"dog-${i}").toList.toDF("animalType").rdd
    }
//    animalsRdd.collect().foreach(println)

    animalsRdd.repartition(partitions).map { row =>
      println(s"THE row is $row / and first element is ${row.get(0).toString}")
      val animal = row.get(0).toString
      println(s"animal: $animal")
    }

    val sumOfScores = {
      val startTime = Instant.now.toEpochMilli
      val sum = 276 // calculateSumOfScores(partitions, animalsRdd)
      val endTime = Instant.now.toEpochMilli
      val result: Long = (endTime - startTime)
      println(result)
      if (result > 2000) {
        throw new RuntimeException("Too slow !  Queue of animals to be tested may overflow. And animals will be sad !")
      }
      sum
    }

    System.out.println("sumOfScores :" + sumOfScores);
    assert(sumOfScores == 276)
  }

//  private def calculateSumOfScores(partitions: Int, animalsRdd: RDD[Row]) = {
//    val results: RDD[fixed.AnimalTestResult] = animalsRdd.repartition(partitions).map { row =>
//      println(s"THE row is $row / and first element is ${row.get(0).toString}")
//      val animal = row.get(0).toString
//      val sessionId = AnimalWeighingService.establishSession
//      fixed.AnimalTestResult(animal, AnimalWeighingService.testSubjectAndReportScore(sessionId, animal))
//    }
//    results.collect().map(_.score).sum
//  }
}
/*
## Use in Notebook/Worksheet
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
 */