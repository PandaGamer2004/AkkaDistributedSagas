package com.transactor.persistence.actors



import akka.actor.typed._
import akka.actor.typed.scaladsl._
import scala.concurrent.duration._
import akka.pattern.StatusReply




case class EventStreamIdenitifer(id: Int)

trait EventSerializer[TEvent] {
    //Because we are serializing to persistence and not to wire we can
    //Affor to this on is implemented in order to implement protobuf binary serialization
    //for instance to protobuf
    def serializeToBinary(event: TEvent): Array[Byte]

    def serializeToString(event: TEvent): Array[String]
}

trait EventPersister[TEvent] {
    sealed trait PersisterCommand
    sealed trait PersisterResponse

    case class AcknowlegeAppendToStream(
        stream: EventStreamIdenitifer
    ) extends PersisterCommand

    case class AppendEventsToStream (
        event: Seq[TEvent],
        stream: EventStreamIdenitifer,
        //Status reply ask pattern, implemented to facilitate askWithStatus to ensure reliable storage
        replyTo: ActorRef[StatusReply[AcknowlegeAppendToStream]] 
    ) extends PersisterCommand

    case class LoadEventStreamResult(
        stream: EventStreamIdenitifer,
        loadedEvents: Seq[TEvent]
    )

    case class LoadEventsFromStream(
        streamIdentifier: EventStreamIdenitifer,
        replyTo: ActorRef[StatusReply[PersisterResponse]]
    )
}




case class ConnectionProvider(jdbcConnectionString: String)
object EventPersister {
    implicit def relationalEventPersister[TEvent: EventSerializer](connection: ConnectionProvider): EventPersister[TEvent] = {
        val eventSerializer = implicitly[EventSerializer[TEvent]]

        throw new NotImplementedError();
    }
}

