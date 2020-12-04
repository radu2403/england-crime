package pipeline

import com.typesafe.config.ConfigFactory
import sparksessionmanager.SessionManager
import org.apache.spark.sql.DataFrame

class FactoryPipeline(implicit private val sparkManager: SessionManager) {
  lazy val config = ConfigFactory.load()

  //  The pipeline creation
  def getEtlDag(): BaseDag = {
    new ImportDag(config.getString("mongo.dataStreetPath"),
                  config.getString("mongo.dataOutcomePath"))
  }

  def getCrimeTypeDag(df: DataFrame): BaseDag = {
    new CrimeTypeKPIDag(df)
  }

  def getDistrictDag(df: DataFrame): BaseDag = {
    new DistrictKPIDag(df)
  }
}


