package etl.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}

trait BaseDag {
  def getDag: DataFrame
  def writeDataFrame(df: DataFrame)
  def end
}
