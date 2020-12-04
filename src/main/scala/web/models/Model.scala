package web.models

import io.circe.syntax._
import io.circe._

import org.bson.types.ObjectId

case class FindByIdRequest(id: String) {
  require(ObjectId.isValid(id), "the informed id is not a representation of a valid hex string")
}

case class FindAllByPage(page: String) {
  require(page.forall(_.isDigit) && page.toInt > 0, "The page number should be greater the 0")

}

case class Felony(_id: ObjectId,
                  crimeID: String,
                  districtName: String,
                  latitude: String,
                  longitude: String,
                  crimeType: String,
                  lastOutcome: String	)

object Felony {
  implicit val encoder: Encoder[Felony] = (f: Felony) => {
    Json.obj(
      "id" -> f._id.toHexString.asJson,
      "crimeID" -> f.crimeID.asJson,
      "districtName" -> f.districtName.asJson,
      "latitude" -> f.latitude.asJson,
      "longitude" -> f.longitude.asJson,
      "crimeType" -> f.crimeType.asJson,
      "lastOutcome" -> f.lastOutcome.asJson
    )
  }

  implicit val decoder: Decoder[Felony] = (c: HCursor) => {
    for {
      crimeID <- c.downField("crimeID").as[String]
      districtName <- c.downField("districtName").as[String]
      latitude <- c.downField("latitude").as[String]
      longitude <- c.downField("longitude").as[String]
      crimeType <- c.downField("crimeType").as[String]
      lastOutcome <- c.downField("lastOutcome").as[String]
    } yield Felony(ObjectId.get(), crimeID, districtName, latitude, longitude, crimeType, lastOutcome)
  }
}

case class CrimeTypeStats(_id: ObjectId, crimeType: String, count: Int)

object CrimeTypeStats {
  implicit val encoder: Encoder[CrimeTypeStats] = (f: CrimeTypeStats) => {
    Json.obj(
      "id" -> f._id.toHexString.asJson,
      "crimeType" -> f.crimeType.asJson,
      "count" -> f.count.asJson
    )
  }

  implicit val decoder: Decoder[CrimeTypeStats] = (c: HCursor) => {
    for {
      crimeType <- c.downField("crimeType").as[String]
      count <- c.downField("count").as[Int]
    } yield CrimeTypeStats(ObjectId.get(), crimeType, count)
  }
}

case class DistrictStats(_id: ObjectId, districtName: String, count: Int)

object DistrictStats {
  implicit val encoder: Encoder[DistrictStats] = (f: DistrictStats) => {
    Json.obj(
      "id" -> f._id.toHexString.asJson,
      "districtName" -> f.districtName.asJson,
      "count" -> f.count.asJson
    )
  }

  implicit val decoder: Decoder[DistrictStats] = (c: HCursor) => {
    for {
      districtName <- c.downField("districtName").as[String]
      count <- c.downField("count").as[Int]
    } yield DistrictStats(ObjectId.get(), districtName, count)
  }
}


case class User(_id: ObjectId, username: String, age: Int) {
  require(username != null, "username not informed")
  require(username.nonEmpty, "username cannot be empty")
  require(age > 0, "age cannot be lower than 1")
}

object User {
  implicit val encoder: Encoder[User] = (a: User) => {
    Json.obj(
      "id" -> a._id.toHexString.asJson,
      "username" -> a.username.asJson,
      "age" -> a.age.asJson
    )
  }

  implicit val decoder: Decoder[User] = (c: HCursor) => {
    for {
      username <- c.downField("username").as[String]
      age <- c.downField("age").as[Int]
    } yield User(ObjectId.get(), username, age)
  }
}

case class Message(message: String)

object Message {
  implicit val encoder: Encoder[Message] = m => Json.obj("message" -> m.message.asJson)
}