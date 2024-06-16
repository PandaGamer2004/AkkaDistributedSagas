package com.transactor.persistence.actors.models

import java.util.UUID

type ChannelId = UUID
type EventId = Int


//Make this case class available only for the current translation unit
case class EventPersistenceModel[TEventPayload](
    eventId: EventId, 
    channelId: ChannelId, 
    eventPayload :TEventPayload
)
