package com.transactor.persistence.actors

import akka.actor.typed._
import akka.actor.typed.scaladsl._
import scala.concurrent.duration._
import akka.pattern.StatusReply
import com.transactor.persistence.actors.models.ChannelId
import com.transactor.persistence.actors.models.EventPersistenceModel
import com.transactor.persistence.actors.repositories.PostgresEventRepository
import com.transactor.serialization.SerDe
import com.transactor.serialization.SerializedJson
import scala.util.Success
import scala.meta.internal.javacp.BaseType.F
import io.getquill.norm.EmptyProductQuatBehavior.Fail
import scala.util.Failure
import fansi.ErrorMode.Throw




object EventPersister {
  sealed trait PersisterCommand[TPayload]
  sealed trait PersisterResponse[TPayload]



  case object LoadEventsFailed{}

  case class LoadEventsResponse[TPayload](
    result: Either[List[EventPersistenceModel[TPayload]], LoadEventsFailed.type],
  ) extends PersisterResponse[TPayload]

  private case class LoadEventsResult[TPayload]
  (
    channelsId: ChannelId,
    result: Either[List[EventPersistenceModel[TPayload]], Throwable],
    notifyTo: ActorRef[LoadEventsResponse[TPayload]]
  )  extends PersisterCommand[TPayload]

  case class LoadEventsRequest[TPayload](
    channelId: ChannelId, 
    replyTo: ActorRef[LoadEventsResponse[TPayload]]
  ) extends PersisterCommand[TPayload]



  case object StoreEventsSuccess{}
  case object StoreEventsFailed{}

  case class StoreEventsResponse[TPayload](
    result: Either[StoreEventsSuccess.type, StoreEventsFailed.type],
  ) extends PersisterResponse[TPayload]


  private case class StoreEventsResult[TPayload](
    result: Either[Unit, Throwable],
    notyify: ActorRef[StoreEventsResponse[TPayload]]
  ) extends PersisterCommand[TPayload]


  case class StoreEventsRequest[TPayload](
    eventsToStore: List[EventPersistenceModel[TPayload]],
    replyTo: ActorRef[StoreEventsResponse[TPayload]]
  ) extends PersisterCommand[TPayload]


  def makeForType[TPayload](implicit serDe: SerDe[TPayload, SerializedJson]): Behavior[PersisterCommand[TPayload]] = Behaviors.receive{
    (context, message) => {
      message match {
        case StoreEventsRequest(eventsToStore, replyTo) => {
          val postgresRepository = PostgresEventRepository.getInstance

          val storeEventsResult = postgresRepository
                  .storeEvents(eventsToStore)

          context.pipeToSelf(storeEventsResult){
            case Success(value) => value match{
              case Success(value) => StoreEventsResult(
                result = Left(()),
                notyify = replyTo
              )
              case Failure(exception) => StoreEventsResult(
                result = Right(exception),
                notyify = replyTo
              )
            }
            case Failure(exception) =>  StoreEventsResult[TPayload](
              result = Right(exception),
              replyTo
            )
          }
          Behaviors.same
        }
        case StoreEventsResult(result, notyify) => {
          result match {
            case Left(value) => notyify ! StoreEventsResponse(
              result = Left(StoreEventsSuccess)
            )
            case Right(value) => notyify ! StoreEventsResponse(
              result = Right(StoreEventsFailed)
            )
          }
          Behaviors.stopped
        }
        
        case LoadEventsRequest(channelId, replyTo) => 
          val postgresRepository = PostgresEventRepository.getInstance
          val loadEventsOperation = 
            postgresRepository.loadEventsByChannelId(channelId)

          context.pipeToSelf(loadEventsOperation){
            case Success(value) => value match {
              case Failure(exception) => LoadEventsResult(
                channelId,
                Right(exception),
                replyTo
              )
              case Success(value) => LoadEventsResult(
                channelId,
                Left(value),
                replyTo
              )
            }
            case Failure(exception) => LoadEventsResult(
              channelId,
              Right(exception),
              replyTo
            )
          }
          Behaviors.same

        case LoadEventsResult(
          channelsId, 
          res, 
          notify
        ) => {
            res match {
              case Left(value) => notify ! LoadEventsResponse(Left(value))
              case Right(value) => notify ! LoadEventsResponse(Right(LoadEventsFailed))
            }
            Behaviors.stopped
          }  
      } 
    }
  }

}
