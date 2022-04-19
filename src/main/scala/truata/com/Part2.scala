package truata.com

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import truata.com.config.SparkConfig

import java.io.File
import java.net.URL
import scala.reflect.io
import scala.sys.process._

object Part2 extends SparkConfig with App {

  import sparkSession.implicits._

  val tmpFile = File.createTempFile("pre", "").toString

  new URL(
    "https://github.com/databricks/LearningSparkV2/blob/master/mlflow-project-example/data/sf-airbnb-clean.parquet/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet?raw=true"
  ) #> new File(
    tmpFile
  ) !!

  val airbnb = sparkSession.read.load(tmpFile)

  val out_2_2 = "out/out_2_2.txt"
  io.File(out_2_2).deleteRecursively()
  airbnb
    .agg(min($"price").as("min_price"), max($"price").as("max_price"), count(lit("1")).as("count"))
    .coalesce(1)
    .write
    .format("csv")
    .option("header", true)
    .save(out_2_2)

  val out_2_3 = "out/out_2_3.txt"
  io.File(out_2_3).deleteRecursively()
  airbnb
    .filter($"price" > 5000 && $"review_scores_value" === "10")
    .agg(avg($"bathrooms").as("avg_bathrooms"), avg($"bedrooms").as("avg_bedrooms"))
    .coalesce(1)
    .write
    .format("csv")
    .option("header", true)
    .save(out_2_3)

  val out_2_4 = "out/out_2_4.txt"
  io.File(out_2_4).deleteRecursively()
  airbnb
    .withColumn("rank", row_number().over(Window.orderBy($"price".asc, $"review_scores_rating".desc)))
    .filter($"rank" === "1")
    .select("accommodates")
    .coalesce(1)
    .write
    .format("csv")
    .option("header", true)
    .save(out_2_4)

}
