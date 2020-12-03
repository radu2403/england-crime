package etl.pipeline

import etl.sparksessionmanager.SessionManager
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.functions._


class CrimeTypeKPIDag(df: DataFrame)(implicit private val sparkManager: SessionManager) extends BaseDag {
  val spark = sparkManager.spark
  import spark.implicits._

  val COLLECTION_NAME = sys.env.getOrElse("CRIME_TYPE_COLLECTION_NAME", "crime_type_stats")

  override def getDag: DataFrame =
    // count
    df.groupBy("crimeType")
      .agg(count($"crimeID").as("count"))
      .orderBy($"count".desc)

  override def writeDataFrame(df: DataFrame): Unit = sparkManager.write(df, COLLECTION_NAME)

  override def end: Unit = sparkManager.stop
}

class DistrictKPIDag(df: DataFrame)(implicit private val sparkManager: SessionManager) extends BaseDag {
  val spark = sparkManager.spark
  import spark.implicits._

  val COLLECTION_NAME = sys.env.getOrElse("DISTRICT_COLLECTION_NAME", "district_stats")

  override def getDag: DataFrame =
    df.groupBy("districtName")
      .agg(count($"crimeID").as("count"))
      .orderBy($"count".desc)


  override def writeDataFrame(df: DataFrame): Unit = sparkManager.write(df, COLLECTION_NAME)

  override def end: Unit = sparkManager.stop
}
