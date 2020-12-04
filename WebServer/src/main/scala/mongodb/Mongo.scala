package mongodb


import com.typesafe.config.ConfigFactory
import org.bson.codecs.configuration.CodecRegistries._
import org.mongodb.scala._
import models.{CrimeTypeStats, DistrictStats, Felony, User}
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._

object Mongo {
  lazy val config = ConfigFactory.load()
  lazy val mongoClient: MongoClient = MongoClient(config.getString("mongo.uri"))
  lazy val codecRegistry = fromRegistries(fromProviders(classOf[User]), DEFAULT_CODEC_REGISTRY)
  lazy val database: MongoDatabase = mongoClient.getDatabase(config.getString("mongo.database")).withCodecRegistry(codecRegistry)

  lazy val userCollection: MongoCollection[User] = database.getCollection[User]("users")
  lazy val felonyCollection: MongoCollection[Felony] = database.getCollection[Felony]("felonies")
  lazy val districtStatsCollection: MongoCollection[DistrictStats] = database.getCollection[DistrictStats]("district_stats")
  lazy val crimeTypeCollection: MongoCollection[CrimeTypeStats] = database.getCollection[CrimeTypeStats]("crime_type_stats")
}