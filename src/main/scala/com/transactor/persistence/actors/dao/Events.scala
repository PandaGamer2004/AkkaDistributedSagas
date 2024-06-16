package com.transactor.persistence.actors.dao

import java.util.UUID

case class Events(id: Int, channelId: UUID, eventData: String);

case class EventsInsertionModel(channelId: UUID, payload: String) 



