package web.models.repository

import org.mongodb.scala.MongoCollection
import web.models.Felony

import scala.concurrent.{ExecutionContext, Future}

class FelonyRepository(collection: MongoCollection[Felony])(implicit ec: ExecutionContext) {

  val valuesPerPage = 100

  def findAll(pageNumber: Int): Future[Option[Felony]] = {
    println("************in felony****************")
    collection.find()
              .skip(valuesPerPage * (pageNumber-1))
              .limit(valuesPerPage)
              .map(Option(_)).head()
  }

  //              .toFuture()

}
