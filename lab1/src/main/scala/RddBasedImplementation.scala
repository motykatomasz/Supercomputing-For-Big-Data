import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RddBasedImplementation {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder.appName("Lab 1 RDD implementation").config("spark.master", "local").getOrCreate()
    val sc = spark.sparkContext

    val rawRDD = sc.textFile("./data/segment100/*.gkg.csv")

    val t0 = System.currentTimeMillis()

    val splittedColumns = rawRDD.map(_.split("\t",-1))

    val filteredColumns = splittedColumns.filter(_.length == 27).map(row => (row(1).substring(0,8), row(23).split(";")))

    val splittedNamesColumn = filteredColumns.mapValues(names => names.map(_.split(",")))

    val addedOnesToNames = splittedNamesColumn.flatMapValues(a => a.filter(a => (a(0) != "" && !a(0).contains("Category"))).map(a => (a(0), 1)))

    val preparedForCounting = addedOnesToNames.map{ case (date: String, (name: String, count: Int)) =>
      ((date, name), count)
    }

    val countedNamesByDate = preparedForCounting.reduceByKey(_+_).map{ case ((date: String, name: String), count: Int) =>
      (date, (name, count))
    }.groupByKey()

    val sorted = countedNamesByDate.mapValues(l => l.toList.sortBy(_._2)(Ordering[Int].reverse).take(10))

    sorted.collect()

    val t1 = System.currentTimeMillis()

    println("Elapsed time: " + (t1 - t0) + "ms")

    spark.stop()
  }
}
