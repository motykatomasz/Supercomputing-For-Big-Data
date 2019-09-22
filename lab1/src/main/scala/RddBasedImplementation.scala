import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RddBasedImplementation {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder.appName("Lab 1 RDD implementation").config("spark.master", "local[*]").getOrCreate()
    val sc = spark.sparkContext

    val rawRDD = sc.textFile("./data/segment500/*.gkg.csv")

    val t0 = System.currentTimeMillis()

    //Split row by tabs
    val splittedColumns = rawRDD.map(_.split("\t",-1))

    //Filter rows that doesn't have 27 columns and discard all but Date and Allnames
    //Also split AllNames => (Date, Array[Names])
    val filteredColumns = splittedColumns.filter(_.length == 27).map(row => (row(1).substring(0,8), row(23).split(";")))

    //Split Names with numbers associated with them
    val splittedNamesColumn = filteredColumns.mapValues(names => names.map(_.split(",")))

    //Filter empty names and those that contain "Category" because they are not actually contributing to the count
    //Flatten the names and add 1 to each name => (Date, (Name, 1))
    val addedOnesToNames = splittedNamesColumn.flatMapValues(a => a.filter(a => (a(0) != "" && !a(0).contains("Category"))).map(a => (a(0), 1)))

    //Prepare for reducing
    val preparedForCounting = addedOnesToNames.map{ case (date: String, (name: String, count: Int)) =>
      ((date, name), count)
    }

    //Count apperences of each name for certain date and group them by date.
    val countedNamesByDate = preparedForCounting.reduceByKey(_+_).map{ case ((date: String, name: String), count: Int) =>
      (date, (name, count))
    }.groupByKey()

    //Sort and take first 10 for each date
    val sorted = countedNamesByDate.mapValues(l => l.toList.sortBy(_._2)(Ordering[Int].reverse).take(10))

    sorted.collect().foreach(println)

    val t1 = System.currentTimeMillis()

    println("Elapsed time: " + (t1 - t0) + "ms")

    spark.stop()
  }
}
