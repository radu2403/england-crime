package etl

import etl.pipeline.FactoryPipeline
import etl.sparksessionmanager.SessionManagerFactory
import org.apache.spark.sql.SparkSession


object Program {

  def main(args: Array[String]) = {
    implicit val sparkManager = SessionManagerFactory.createMongoSessionManager()
    val pipelineFactory = new FactoryPipeline()

    val dag = pipelineFactory.getEtlDag()

    val df = dag.getDag

    println(df.show(10))

    dag.writeDataFrame(df)
    dag.end
  }

}