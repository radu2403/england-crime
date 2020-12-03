package etl.pipeline

import etl.sparksessionmanager.{MongoSessionManager, SessionManager, SessionManagerFactory}

class FactoryPipeline(implicit private val sparkManager: SessionManager) {
  private val STREET_PATH = sys.env.getOrElse("DATA_STREET_PATH", "./data/*/*-street.csv")
  private val OUTCOME_PATH = sys.env.getOrElse("DATA_OUTCOME_PATH", "./data/*/*-outcomes.csv")

  //  The pipeline creation
  def getEtlDag(): BaseDag = {
    new ImportDag(STREET_PATH, OUTCOME_PATH)
  }
}


