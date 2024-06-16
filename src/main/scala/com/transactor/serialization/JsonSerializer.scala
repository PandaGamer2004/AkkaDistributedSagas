package com.transactor.serialization


import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import com.transactor.serialization.SerDe



object JsonSerializers {
    def jsonSerDe[TEventPayload: Decoder: Encoder]: SerDe[TEventPayload, SerializedJson] = new SerDe[TEventPayload, SerializedJson] {
        override def deserialize(s: SerializedJson): TEventPayload = {
            decode[TEventPayload](s) match {
                case Right(value) => value
                case Left(error) => throw new RuntimeException(s"Deserialization failed: $error")
            }
        }

        override def serialize(in: TEventPayload): SerializedJson = {
        in.asJson.noSpaces
        }
    }
}