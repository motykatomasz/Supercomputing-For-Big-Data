import java.sql.Timestamp
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object DataFrameRepresentationV2 {
  case class NewsArticle (
                           GKGRECORDID: String,
                           DATE: Timestamp,
                           SourceCollectionIdentifier: Integer,
                           SourceCommonName: String,
                           DocumentIdentifier: String,
                           Counts: String,
                           V2Counts: String,
                           Themes: String,
                           V2Themes: String,
                           Locations: String,
                           V2Locations: String,
                           Persons: String,
                           V2Persons: String,
                           Organizations: String,
                           V2Organizations: String,
                           V2Tone: String,
                           Dates: String,
                           GCAM: String,
                           SharingImage: String,
                           RelatedImages: String,
                           SocialImageEmbeds: String,
                           SocialVideoEmbeds: String,
                           Quotations: String,
                           Allnames: String,
                           Amounts: String,
                           TranslationInfo: String,
                           Extras: String
                         )


  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val schema = StructType(
      Array(
        StructField("GKGRECORDID", StringType),
        StructField("DATE", TimestampType),
        StructField("SourceCollectionIdentifier", IntegerType),
        StructField("SourceCommonName", StringType),
        StructField("DocumentIdentifier", StringType),
        StructField("Counts", StringType),
        StructField("V2Counts", StringType),
        StructField("Themes", StringType),
        StructField("V2Themes", StringType),
        StructField("Locations",StringType),
        StructField("V2Locations",StringType),
        StructField("Persons",StringType),
        StructField("V2Persons",StringType),
        StructField("Organizations",StringType),
        StructField("V2Organizations",StringType),
        StructField("V2Tone", StringType),
        StructField("Dates",StringType),
        StructField("GCAM", StringType),
        StructField("SharingImage", StringType),
        StructField("RelatedImages",StringType),
        StructField("SocialImageEmbeds",StringType),
        StructField("SocialVideoEmbeds",StringType),
        StructField("Quotations", StringType),
        StructField("AllNames", StringType),
        StructField("Amounts",StringType),
        StructField("TranslationInfo",StringType),
        StructField("Extras", StringType)
      )
    )

    val spark = SparkSession
      .builder
      .appName("Lab 1 DF(2) implementation")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val sc = spark.sparkContext

    val ds = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .option("timestampFormat", "yyyyMMddHHmmSS")
      .schema(schema)
      .load("./data/segment100/*.gkg.csv")
      .as[NewsArticle]

    val t0 = System.currentTimeMillis()

    // remove rows whose AllNames attribute is null
    val ds_filtered = ds.filter(x => x.Allnames != null)

    // only use DATE and AllNames
    val useful_columns = ds_filtered.map(x => (x.DATE, x.Allnames))

    // split the names that are present in each row; the second column now is a list of splitted elements (each one being (name,number))
    val splitNames = useful_columns.map(x => (x._1, x._2.split(";")))

    // flatmap the splitted elements in the list, so now we have [date, (name, number)]
    // also remove the entries that contain "Category" because they are not actually contributing to the count
    val dateAndNames = splitNames.flatMap(x => x._2.map(name => (x._1, name)))
      .filter(x => !x._2.contains("Category"))

    // removing the hour from the date
    // we go from yyyy-mm-dd hh:mm:ss to only yyyy-mm-dd
    val removeHour = dateAndNames
      .map(x => (x._1.toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),x._2))

    // remove the numbers from the name field; again we have a list of splitted elements, now with the shape [name, number, name, number, etc.]
    val splitNumbers = removeHour.map(x => (x._1, x._2.split(",")))

    // flatmap the list and keep only the entries without numbers in it
    val noNumber = splitNumbers.flatMap(x => x._2.map(name => (x._1, name)))
      .filter(x => !x._2.matches("([0-9]+)"))

    // for each name count how many times it is present in the dataframe
    val groupedNames = noNumber.groupByKey(x => x).count()


    // partition by date and find the rank in each day window
    val result = groupedNames
      .withColumn("order", rank.over(Window.partitionBy("key._1").orderBy($"count(1)".desc)))
      .filter(col("order") <= 10)

    result.collect()

    val t1 = System.currentTimeMillis()

    // display the list of names
    println("Elapsed time: " + (t1 - t0) + "ms")

    spark.stop()
  }
}
