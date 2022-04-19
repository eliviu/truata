package truata.com
import truata.com.config.SparkConfig

import java.io.File
import java.net.URL
import scala.reflect.io
import scala.sys.process._

object Part1 extends SparkConfig with App {

  val tmpFile = File.createTempFile("pre", ".csv").toString

  new URL("https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/groceries.csv") #> new File(
    tmpFile
  ) !!

  val groceriesRdd = sparkSession.sparkContext.textFile(tmpFile)

  val out_1_2a = "out/out_1_2a.txt"
  io.File(out_1_2a).deleteRecursively()
  groceriesRdd.flatMap(_.split(",")).distinct().coalesce(1).saveAsTextFile(out_1_2a)

  val out_1_2b = "out/out_1_2a.txt"
  io.File(out_1_2b).deleteRecursively()
  val count = groceriesRdd.flatMap(_.split(",")).distinct().count()
  reflect.io.File(out_1_2b).writeAll(s"Count: \n ${count.toString}")

  val out_1_3 = "out/out_1_3.txt"
  io.File(out_1_3).deleteRecursively()
  groceriesRdd
    .flatMap(_.split(","))
    .distinct()
    .groupBy(x => x)
    .map { case (k, v) => (k, v.size) }
    .sortBy(_._2, false)
    .coalesce(1)
    .saveAsTextFile(out_1_3)

}
