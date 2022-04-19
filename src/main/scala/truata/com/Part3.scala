package truata.com

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{ StringIndexer, VectorAssembler }
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }
import truata.com.config.SparkConfig

import java.io.File
import java.net.URL
import scala.reflect.io
import scala.sys.process._

object Part3 extends SparkConfig with App {

  import sparkSession.implicits._

  val tmpFile = File.createTempFile("irisdata", ".csv").toString

  new URL(
    "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
  ) #> new File(
    tmpFile
  ) !!

  val irisSchema = StructType(
    Array(
      StructField("sepal_length", DoubleType, true),
      StructField("sepal_width", DoubleType, true),
      StructField("petal_length", DoubleType, true),
      StructField("petal_width", DoubleType, true),
      StructField("class", StringType, true)
    )
  )

  val irisData = sparkSession.read.schema(irisSchema).csv(tmpFile)

  val featureCols = Array("sepal_length", "sepal_width", "petal_length", "petal_width")

  // VectorAssembler with feature col
  val assembler = new VectorAssembler()
    .setInputCols(featureCols)
    .setOutputCol("features")
  val featureDf = assembler.transform(irisData)

  //map string class col to label numeric col, and get labels array
  val indexer = new StringIndexer()
    .setInputCol("class")
    .setOutputCol("label")
  val indexerModel = indexer.fit(featureDf)
  val labelDf = indexerModel.transform(featureDf)
  val labelsArray = indexerModel.labelsArray(0)

  //train logistic regression model
  val logisticRegression =
    (new LogisticRegression()).setFeaturesCol("features").setLabelCol("label").setFamily("multinomial").setMaxIter(10)
  val logisticRegressionModel = logisticRegression.fit(labelDf)

  //create test dataset
  val testDataSeq = Seq((5.1, 3.5, 1.4, 0.2), (6.2, 3.4, 5.4, 2.3))
  val testDataDF = sparkSession.sparkContext
    .parallelize(testDataSeq)
    .toDF("sepal_length", "sepal_width", "petal_length", "petal_width")
  val testDataDfFeatures = assembler.transform(testDataDF)

  // get predicted label and class on test dataset
  val getClassFromLabel: UserDefinedFunction = udf((label: Int) => labelsArray(label))
  val predictionDf = logisticRegressionModel
    .transform(testDataDfFeatures)
    .withColumn("class", getClassFromLabel($"prediction"))
  predictionDf.show()

  //write prediction results to a CSV file out/out_3_2.txt
  val out_3_2 = "out/out_3_2.txt"
  io.File(out_3_2).deleteRecursively()
  predictionDf
    .select("class")
    .coalesce(1)
    .write
    .format("csv")
    .option("header", true)
    .save(out_3_2)

}
