package pipeline
import java.io.File

import com.typesafe.config.ConfigFactory
import sparksessionmanager.SessionManager
import org.apache.spark.sql.functions.{count, input_file_name, udf, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

class ImportDag(val streetDataPath: String, val outcomeDataPath: String)(implicit private val sparkManager: SessionManager) extends BaseDag {
  val spark = sparkManager.spark
  import spark.implicits._

  lazy val config = ConfigFactory.load()

  // Transformation DAG
  override def getDag(): DataFrame = {

    //    Ingest
    var streetDf   = ingest(spark, streetDataPath)
    var outcomesDf = ingest(spark, outcomeDataPath)

    //    Transform
    //      *** select the minimum fields
    streetDf   = selectStreetColumns(streetDf)
    outcomesDf = selectOutcomesColumns(outcomesDf)

    //     *** remove NULL IDs
    streetDf   = removeNullOnCrimeId(streetDf)
    outcomesDf = removeNullOnCrimeId(outcomesDf)

    //     *** Remove duplicate IDs
    streetDf   = removeDuplicate(streetDf)
    outcomesDf = removeDuplicate(outcomesDf)

    //   *** Add "district name" from file
    streetDf = addDistrictName(streetDf)

    //    *** Join the street with the outcome to provide the final result
    createFinalDf(streetDf, outcomesDf)
  }

  protected def createFinalDf(streetDf: DataFrame, outcomesDf: DataFrame): DataFrame = {
    streetDf.join(outcomesDf, Seq("crimeID"), "left")
            .withColumn("lastOutcome",
                        when($"outcomeType".isNotNull, $"outcomeType").otherwise($"lastOutcomeCategory")
            )
            .select($"crimeID".cast(StringType),
                    $"districtName".cast(StringType),
                    $"latitude".cast(StringType),
                    $"longitude".cast(StringType),
                    $"crimeType".cast(StringType),
                    $"lastOutcome".cast(StringType)
            )
  }

  protected def addDistrictName(df: DataFrame): DataFrame = {
    // extract function
    val extractName = udf((path: String) => new File(path).getName().split("-").drop(2).dropRight(1).mkString(" "))

    // DF with extract
     df.withColumn("districtName", extractName($"districtName"))
  }

  protected def removeDuplicate(df: DataFrame): DataFrame = {
    // multiple IDs
    val multiIdDf = df.groupBy($"crimeID")
      .agg(count($"crimeID").as("count"))
      .where($"count" > 1)
      .select($"crimeID")
      .withColumnRenamed("crimeID", "crimeIdMulti")

    // filter out duplicate IDs
    df.join(multiIdDf, $"crimeID" === $"crimeIdMulti", "leftanti")
  }

  protected def removeNullOnCrimeId(df: DataFrame): DataFrame = df.where($"crimeId".isNotNull)

  protected def selectOutcomesColumns(outcomesDf: DataFrame): DataFrame =
    outcomesDf.select($"Crime ID".as("crimeId"),
                      $"Outcome type".as("outcomeType")
    )


  protected def selectStreetColumns(streetDf: DataFrame): DataFrame =
    streetDf.select($"Crime ID".as("crimeID"),
                    $"districtName",
                    $"Latitude".as("latitude"),
                    $"Longitude".as("longitude"),
                    $"Crime type".as("crimeType"),
                    $"Last outcome category".as("lastOutcomeCategory")
    )

  protected def ingest(spark: SparkSession, csvPath: String): DataFrame =
    spark
      .read
      .option("header", "true")
      .csv(csvPath)
      .withColumn("districtName", input_file_name())
//    .limit(1000)

  // Write DAG
  override def writeDataFrame(df: DataFrame): Unit = sparkManager.write(df, collectionName = config.getString("mongo.rawDataCollection") )

  // Close all things
  override def end: Unit = sparkManager.stop
}
