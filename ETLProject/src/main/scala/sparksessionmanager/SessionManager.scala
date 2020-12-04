package sparksessionmanager

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SessionManager {
  def spark: SparkSession
  def write(df: DataFrame, collectionName: String)
  def stop
}

class MongoSessionManager(private val sparkSession: SparkSession) extends SessionManager {

  override def spark: SparkSession = sparkSession

  override def write(df: DataFrame, collectionName: String): Unit = {
    df.write
      .option("collection", collectionName)
      .mode("overwrite")
      .format("mongo")
      .save()
  }

  override def stop: Unit = spark.stop()
}