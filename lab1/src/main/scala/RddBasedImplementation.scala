import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RddBasedImplementation {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder.appName("Lab 1 RDD implementation").config("spark.master", "local").getOrCreate()

    // ...

    spark.stop()
  }
}
