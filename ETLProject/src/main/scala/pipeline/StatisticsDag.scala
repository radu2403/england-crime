package pipeline

import com.typesafe.config.ConfigFactory
import sparksessionmanager.SessionManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


class CrimeTypeKPIDag(df: DataFrame)(implicit private val sparkManager: SessionManager) extends BaseDag {
  val spark = sparkManager.spark
  import spark.implicits._

  lazy val config = ConfigFactory.load()

  override def getDag: DataFrame =
    // count
    df.groupBy("crimeType")
      .agg(count($"crimeID").as("count"))
      .orderBy($"count".desc)

  override def writeDataFrame(df: DataFrame): Unit = sparkManager.write(df, config.getString("mongo.crimeTypeStatsCollection"))

  override def end: Unit = sparkManager.stop
}

class DistrictKPIDag(df: DataFrame)(implicit private val sparkManager: SessionManager) extends BaseDag {
  val spark = sparkManager.spark
  import spark.implicits._
  lazy val config = ConfigFactory.load()

  override def getDag: DataFrame =
    df.groupBy("districtName")
      .agg(count($"crimeID").as("count"))
      .orderBy($"count".desc)


  override def writeDataFrame(df: DataFrame): Unit = sparkManager.write(df, config.getString("mongo.districtStatsCollection"))

  override def end: Unit = sparkManager.stop
}
