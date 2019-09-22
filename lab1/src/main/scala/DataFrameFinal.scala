import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object DataFrameFinal {
  case class NewsArticle (
                           GKGRECORDID: String,
                           DATE: String,
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
        StructField("DATE", StringType),
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
      .appName("Lab 1 DF(4) implementation")
      .config("spark.master", "local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val sc = spark.sparkContext

    val ds = spark.read
      .format("csv")
      .option("delimiter", "\t")
      .schema(schema)
      .load("./data/segment500/*.gkg.csv")
      .as[NewsArticle]

    val t0 = System.currentTimeMillis()

    // remove rows whose AllNames attribute are not null
    val dsFiltered = ds.filter(col("AllNames").isNotNull)

    // removing the hour from the date
    // we go from yyyymmddhhmmss to only yyyymmdd
    val removeHour = dsFiltered.select(substring(col("DATE"), 0, 8).as("DATE"), col("AllNames"))

    // remove number associated with each name
    val removeNumber = removeHour.select(col("DATE"), regexp_replace($"AllNames" ,"(,[0-9]+)", "").as("AllNames"))

    // split the names that are present in each row
    // now we have schema = | date | single name |
    val splitNames = removeNumber.select(col("DATE"), explode(split(col("AllNames"), ";")).as("Name"))

    // remove the entries that contain "Category" because they are not actually contributing to the count
    val filterCategory = splitNames.filter(!col("Name").contains("Category"))

    // count occurence of each name by day
    val countNames = filterCategory.groupBy("DATE", "Name").count

    // order names within each day and take 10 first
    val take10Best = countNames.withColumn("Order", rank.over(Window.partitionBy("DATE").orderBy($"count".desc)))
      .filter(col("Order") <= 10)

    take10Best.collect().foreach(println)

    val t1 = System.currentTimeMillis()

    // display the list of names
    println("Elapsed time: " + (t1 - t0) + "ms")

    spark.stop()
  }
}
