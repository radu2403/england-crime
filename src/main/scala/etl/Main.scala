package etl

import etl.pipeline.FactoryPipeline
import etl.sparksessionmanager.SessionManagerFactory
import org.apache.spark.sql.SparkSession


object Program {

  def main(args: Array[String]) = {
    implicit val sparkManager = SessionManagerFactory.createMongoSessionManager()
    val pipelineFactory = new FactoryPipeline()

    println("**** Creating import manager.....")
    val importManager = pipelineFactory.getEtlDag()
    println("***** Import manager created!")

    try {

      println("**** Creating DAG.....")
      val df = importManager.getDag
      println("***** DAG created!")

      // Statistics
      println(df.show(10))
      println("**** FINAL - There are a number of crimeID NULL: " + df.where(df("crimeID").isNull).count)

      println("**** Writing dataframe.....")
      importManager.writeDataFrame(df)
      println("**** Done writing! ")

    } finally {
      importManager.end
    }
  }

}