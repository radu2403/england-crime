package etl.sparksessionmanager

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SessionManager {
  def spark: SparkSession
  def write(df: DataFrame)
  def stop
}

class MongoSessionManager(private val sparkSession: SparkSession) extends SessionManager {

  override def spark: SparkSession = sparkSession

  override def write(df: DataFrame): Unit = {
    df.write
      .option("collection", "hundredClub")
      .mode("overwrite")
      .format("mongo")
      .save()
  }

  override def stop: Unit = spark.stop()
}