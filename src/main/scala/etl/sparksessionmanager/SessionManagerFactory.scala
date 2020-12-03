package etl.sparksessionmanager

import org.apache.spark.sql.SparkSession

object SessionManagerFactory {
  def createMongoSessionManager(): SessionManager = {
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .config("spark.mongodb.input.uri", "mongodb://spark_user:xyz123@127.0.0.1/spark")
      .config("spark.mongodb.output.uri", "mongodb://spark_user:xyz123@127.0.0.1/spark")
      .getOrCreate()

    new MongoSessionManager(spark)
  }

}
