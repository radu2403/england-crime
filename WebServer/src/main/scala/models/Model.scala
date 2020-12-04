package models

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
  implicit val encoder: Encoder[Felony] = (a: Felony) => {
    Json.obj(
      "id" -> a._id.toHexString.asJson,
      "crimeID" -> a.crimeID.asJson,
      "districtName" -> a.districtName.asJson,
      "latitude" -> a.latitude.asJson,
      "longitude" -> a.longitude.asJson,
      "crimeType" -> a.crimeType.asJson,
      "lastOutcome" -> a.lastOutcome.asJson
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

case class CrimeTypeStats(_id: ObjectId, crimeType: String, count: Long)

object CrimeTypeStats {
  implicit val encoder: Encoder[CrimeTypeStats] = (a: CrimeTypeStats) => {
    Json.obj(
      "id" -> a._id.toHexString.asJson,
      "crimeType" -> a.crimeType.asJson,
      "count" -> a.count.asJson
    )
  }

  implicit val decoder: Decoder[CrimeTypeStats] = (c: HCursor) => {
    for {
      crimeType <- c.downField("crimeType").as[String]
      count <- c.downField("count").as[Long]
    } yield CrimeTypeStats(ObjectId.get(), crimeType, count)
  }
}

case class DistrictStats(_id: ObjectId, districtName: String, count: Long)

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
        count <- c.downField("count").as[Long]
      } yield DistrictStats(ObjectId.get(), districtName, count)
    }
}

case class Message(message: String)

object Message {
  implicit val encoder: Encoder[Message] = m => Json.obj("message" -> m.message.asJson)
}