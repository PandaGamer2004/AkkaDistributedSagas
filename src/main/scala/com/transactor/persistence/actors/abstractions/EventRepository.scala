package com.transactor.persistence.actors.abstractions


import scala.concurrent.Future
import scala.util.Try
import com.transactor.persistence.actors.models._
import com.transactor.serialization.{SerDe, SerializedJson}

trait EventRepository {
  def loadEventsByChannelId[TEventPayload](channelId: ChannelId)(implicit
      serDe: SerDe[TEventPayload, SerializedJson]
  ): Future[Try[List[EventPersistenceModel[TEventPayload]]]];

  def storeEvents[TEventPayload](
      eventsToStore: List[EventPersistenceModel[TEventPayload]]
  )(implicit serDe: SerDe[TEventPayload, SerializedJson]): Future[Try[Unit]]
}

