package endpoints

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer

import de.heikoseeberger.akkahttpcirce.ErrorAccumulatingCirceSupport._

import models._
import models.repository._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class FelonyEndpoint(repository: FelonyRepository)(implicit ec: ExecutionContext, mat: Materializer) {

  val felonyRoutes =
    pathPrefix("api" / "felony") {

      (get & path(Segment).as(FindAllByPage)) { request =>
//        onComplete(repository.findAll(request.page.toInt)) {
//          case Success(Some(felony)) =>  complete( "okkkk" )
//        }
        complete("okkkk")
      }
    }


}
