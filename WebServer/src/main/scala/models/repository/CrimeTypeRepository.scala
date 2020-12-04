package models.repository


import models.{CrimeTypeStats, DistrictStats, Felony}
import org.mongodb.scala.MongoCollection

import scala.concurrent.{ExecutionContext, Future}

class CrimeTypeRepository(collection: MongoCollection[CrimeTypeStats])(implicit ec: ExecutionContext)  {
  val valuesPerPage = 100

  def findAll(pageNumber: Int): Future[Seq[Option[CrimeTypeStats]]] = {
    println("************in crime type****************")
    collection.find()
      .skip(valuesPerPage * (pageNumber-1))
      .limit(valuesPerPage)
      .map(Option(_))
      .toFuture()
  }
}

