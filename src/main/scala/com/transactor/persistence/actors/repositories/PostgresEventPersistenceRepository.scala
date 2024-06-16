package com.transactor.persistence.actors.repositories

import io.getquill._

import scala.util.Using
import scala.concurrent.Future
import scala.util.Try
import com.transactor.persistence.actors.connections.ConnectionConfiguration
import com.transactor.persistence.actors.models._
import com.transactor.persistence.actors.dao.*
import com.transactor.serialization.{SerDe, SerializedJson}
import com.transactor.persistence.actors.abstractions.EventRepository
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.UUID


object PostgresEventRepository {
  def getInstance: EventRepository = new:
    override def storeEvents[TEventPayload](
        eventsToStore: List[EventPersistenceModel[TEventPayload]]
    )(implicit serDe: SerDe[TEventPayload, SerializedJson]): Future[Try[Unit]] = {
        Future {
            val postgresConnectionContext = ConnectionConfiguration.configurePostgresConnection()

            val projectedInsertionModel: List[EventsInsertionModel] = 
                eventsToStore.map(
                    e => EventsInsertionModel(e.channelId, serDe.serialize(e.eventPayload)
                    )
            )

            Using(postgresConnectionContext) { ctx => 
                import ctx._


                val insertEventsQuery = quote {
                    liftQuery(projectedInsertionModel)
                        .foreach(e => query[Events]
                                        .insert(
                                            _.channelId -> e.channelId,
                                            _.eventData -> e.payload
                                        )
                                )   
                }
                

                ctx.run(insertEventsQuery)
                ()
            }
        }                    
    }


    def loadEventsByChannelId[TEventPayload](
        channelId: UUID
    )(implicit serDe: SerDe[TEventPayload, SerializedJson]): Future[Try[List[EventPersistenceModel[TEventPayload]]]] = {
      // Using global execution context for now
      Future {
        val postgresConnectionContext = ConnectionConfiguration.configurePostgresConnection()

        Using(postgresConnectionContext) { ctx =>
         import ctx._
    
          //Parametrized query it is just information for u
          //U don't have to worry about
          val loadedEvents: List[Events] = ctx.run(
            query[Events].filter(_.channelId == lift(channelId))
          )

          loadedEvents.map(event =>
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
