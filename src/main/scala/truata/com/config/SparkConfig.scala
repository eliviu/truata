package truata.com.config

import org.apache.spark.sql.SparkSession

trait SparkConfig {

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .config("spark.sql.parquet.compression.codec", "snappy")
    .appName("truata Assesment")
    .master("local[*]")
    .getOrCreate()
}
