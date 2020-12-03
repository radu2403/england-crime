package etl.pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
import java.io.File

import etl.sparksessionmanager.{MongoSessionManager, SessionManager, SessionManagerFactory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, map, udf}
import org.apache.spark.sql.functions._


class FactoryPipeline(implicit private val sparkManager: SessionManager) {
  private val STREET_PATH = sys.env.getOrElse("DATA_STREET_PATH", "./data/*/*-street.csv")
  private val OUTCOME_PATH = sys.env.getOrElse("DATA_OUTCOME_PATH", "./data/*/*-outcomes.csv")

  //  The pipeline creation
  def getEtlDag(): BaseDag = {
    new ImportDag(STREET_PATH, OUTCOME_PATH)
  }
}


