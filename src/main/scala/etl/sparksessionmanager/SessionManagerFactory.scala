package etl.sparksessionmanager

import org.apache.spark.sql.SparkSession

object SessionManagerFactory {
  // TODO: remove in production the default
  val MONGO_USERNAME = sys.env.getOrElse("IMPORT_COLLECTION_NAME", "spark_user")
  val MONGO_PASS = sys.env.getOrElse("IMPORT_COLLECTION_NAME", "xyz123")

  def createMongoSessionManager(): SessionManager = {
    val spark = SparkSession.builder()
      .config("spark.master", "local")
      .config("spark.mongodb.input.uri", s"mongodb://$MONGO_USERNAME:$MONGO_PASS@127.0.0.1/spark")
      .config("spark.mongodb.output.uri", s"mongodb://$MONGO_USERNAME:$MONGO_PASS@127.0.0.1/spark")
      .getOrCreate()

    new MongoSessionManager(spark)
  }

}
