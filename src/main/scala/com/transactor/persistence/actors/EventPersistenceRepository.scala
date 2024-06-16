package com.transactor.persistence.actors

import io.getquill._

import scala.util.Using
import scala.compiletime.ops.string
import scala.concurrent.Future
import scala.util.Try
import com.transactor.persistence.actors.connections.ConnectionConfiguration
import com.transactor.persistence.actors.models._
import com.transactor.persistence.actors.dao.*
import com.transactor.serialization.{SerDe, SerializedJson}
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.UUID

trait EventRepository {
  def loadEventsByChannelId[TEventPayload](channelId: ChannelId)(implicit
      serDe: SerDe[TEventPayload, SerializedJson]
  ): Future[Try[List[EventPersistenceModel[TEventPayload]]]];

  def storeEvents[TEventPayload](
      persistenceModel: List[EventPersistenceModel[TEventPayload]]
  ): Future[Try[Unit]]
}

object PostgresEventRepository {
  def getInstance: EventRepository = new:

    override def storeEvents[TEventPayload](
        persistenceModel: List[EventPersistenceModel[TEventPayload]]
        
    ): Future[Try[Unit]] =  ???


    def loadEventsByChannelId[TEventPayload](
        channelId: UUID
    )(implicit serDe: SerDe[TEventPayload, SerializedJson]): Future[Try[List[EventPersistenceModel[TEventPayload]]]] = {
      // Using global execution context for now
      Future {
        val postgresConnectionContext = ConnectionConfiguration.configurePostgresConnection()

        Using(postgresConnectionContext) { ctx =>
         import ctx._
          // Fully qualify `lift` to avoid ambiguity
        
          val loadedEvents: List[Events] = ctx.run(
            query[Events]
          )


          loadedEvents.filter(e => e.channelId == channelId).map(event =>
            EventPersistenceModel[TEventPayload](
              eventId = event.id,
              channelId = event.channelId,
              eventPayload = serDe.deserialize(event.eventData)
            )
          )
        }
      }
    }

}
