import sparksessionmanager.SessionManagerFactory
import pipeline.FactoryPipeline


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

      // Compute statistics in collections
      // **** crime type
      val crimeTypeKPIManager = pipelineFactory.getCrimeTypeDag(df)
      val dfCrimeTypeKPI = crimeTypeKPIManager.getDag
      crimeTypeKPIManager.writeDataFrame(dfCrimeTypeKPI)

      // **** crime type
      val districtKPIManager = pipelineFactory.getDistrictDag(df)
      val dfDistrictKPI = districtKPIManager.getDag
      districtKPIManager.writeDataFrame(dfDistrictKPI)

    } finally {
      sparkManager.spark.stop()
    }
  }

}