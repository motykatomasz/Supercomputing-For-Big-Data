import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RddBasedImplementation {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder.appName("Lab 1 RDD implementation").config("spark.master", "local").getOrCreate()
    val sc = spark.sparkContext

    val rawRDD = sc.textFile("./data/segment/*.gkg.csv")

    val splittedColumns = rawRDD.map(_.split("\t",-1))

    val filteredColumns = splittedColumns.map(row => (row(1), row(23).split(";")))

    val splittedNamesColumn = filteredColumns.mapValues(names => names.map(_.split(",")))

    val addedOnesToNames = splittedNamesColumn.flatMapValues(a => a.filter(a => a(0) != "").map(a => (a(0), 1)))

    val preparedForCounting = addedOnesToNames.map{ case (date: String, (name: String, count: Int)) =>
      ((date, name), count)
    }

    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

    val countedNamesByDate = preparedForCounting.reduceByKey(_+_).map{ case ((date: String, name: String), count: Int) =>
      (dateFormat.parse(date), (name, count))
    }.groupByKey()

    val sorted = countedNamesByDate.mapValues(l => l.toList.sortBy(_._2)(Ordering[Int].reverse).take(10))

    sorted.foreach(println)

    spark.stop()
  }
}
