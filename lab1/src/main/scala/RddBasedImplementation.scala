import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RddBasedImplementation {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder.appName("Lab 1 RDD implementation").config("spark.master", "local").getOrCreate()

    val sc = spark.sparkContext

    val stringRDD = sc.textFile("./data/segment/*.gkg.csv")

    val splittedColumns = stringRDD.map(_.split("\t",-1))

    val filteredColumnsAndNamesSpiltted = splittedColumns.map(row => (row(1), row(23).split(";")))

    val groupedByDate = filteredColumnsAndNamesSpiltted.reduceByKey((x,y) => x ++ y)

    val mappedValues = groupedByDate.mapValues(names => names.map(_.split(",")))

    val addedOnes = mappedValues.flatMapValues(a => a.map(a => (a(0), 1)))

    val groupedByDateAndName = addedOnes.map{ case (date: String, (name: String, count: Int)) =>
      ((date, name), count)
    }

    val countedByDate = groupedByDateAndName.reduceByKey(_+_).map{ case ((date: String, name: String), count: Int) =>
      (date, (name, count))
    }.groupByKey()

    val sorted = countedByDate.mapValues(l => l.toList.sortBy(_._2)(Ordering[Int].reverse).take(10))

    sorted.foreach(println)

    spark.stop()
  }
}
