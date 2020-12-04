package modules


import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.softwaremill.macwire._
import endpoints.{DistrictEndpoint, Endpoints, FelonyEndpoint, HealthCheckEndpoint}
import models.repository.{DistrictRepository, FelonyRepository}
import mongodb.Mongo

import scala.concurrent.ExecutionContext

class AllModules extends EndpointModule

trait EndpointModule extends AkkaModules with RepositoryModule {
  lazy val healthCheckEndpoint = wire[HealthCheckEndpoint]
//  lazy val userEndpoint = wire[UserEndpoint]
  lazy val felonyEndpoint = wire[FelonyEndpoint]
  lazy val districtEndpoint = wire[DistrictEndpoint]

  lazy val endpoints = wire[Endpoints]
}

trait MongoModule {
  lazy val codecRegistry = Mongo.codecRegistry
//  lazy val userCollection = Mongo.userCollection
  lazy val felonyCollection = Mongo.felonyCollection
  lazy val districtCollection = Mongo.districtStatsCollection
}

trait RepositoryModule extends AkkaModules with MongoModule {
//  lazy val userRepository = wire[UserRepository]
  lazy val felonyRepository = wire[FelonyRepository]
  lazy val districtRepository = wire[DistrictRepository]
}

trait AkkaModules {
  implicit lazy val system = ActorSystem("simpleHttpServerJson")
  implicit lazy val materializer = ActorMaterializer()
  implicit lazy val executor: ExecutionContext = system.dispatcher
}
