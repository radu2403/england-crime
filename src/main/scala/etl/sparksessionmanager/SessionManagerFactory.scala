package etl.sparksessionmanager

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object SessionManagerFactory {
  lazy val config = ConfigFactory.load()

  def createMongoSessionManager(): SessionManager = {
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .config("spark.mongodb.input.uri", config.getString("mongo.uri"))
      .config("spark.mongodb.output.uri", config.getString("mongo.uri"))
      .getOrCreate()

    new MongoSessionManager(spark)
  }

}
