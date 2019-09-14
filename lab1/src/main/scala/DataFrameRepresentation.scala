import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object DataFrameRepresentation {
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
      .appName("GDELThist")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val ds = spark.read
        .format("csv")
        .option("delimiter", "\t")
        .option("timestampFormat", "yyyyMMddHHmmSS")
        .schema(schema)
        .load("C:\\Users\\matte\\Desktop\\TU Delft\\Quarter V\\Supercomputing for Big Data\\SBD-tudelft-master\\lab1\\data\\segment\\*.csv")
        .as[NewsArticle]

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

    // sort in descending order based on the count, go from ((date, name), count) to (date, (name, count)) in order to have the desired shape for the list later
    // then take only the top 10 elements for each date
    val top10NamesPerDate = groupedNames.sort($"count(1)".desc)
        .map{case ((date, name),count) => (date, (name, count))}.rdd
        .groupByKey()
        .mapValues(x => x.toList.take(10))

    // display the list of names
    top10NamesPerDate.collect().foreach(println)

    spark.stop()
  }
}
