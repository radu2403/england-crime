package models.repository

import models.{DistrictStats}
import org.mongodb.scala.MongoCollection

import scala.concurrent.{ExecutionContext, Future}

class DistrictRepository(collection: MongoCollection[DistrictStats])(implicit ec: ExecutionContext)  {
  val valuesPerPage = 100

  def findAll(pageNumber: Int): Future[Seq[Option[DistrictStats]]] = {
    println("************in district****************")
    collection.find()
      .skip(valuesPerPage * (pageNumber-1))
      .limit(valuesPerPage)
      .map(Option(_))
      .toFuture()
//      .head()
  }

}
