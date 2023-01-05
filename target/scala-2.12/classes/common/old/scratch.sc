import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID
import java.time.Instant
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD
val (cores, partitions) = ("1", 1)

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
animalsRdd.collect().foreach(println)

animalsRdd.repartition(partitions).map { row =>
  println(s"THE row is $row / and first element is ${row.get(0).toString}")
  val animal = row.get(0).toString
  println(s"animal: $animal")
}