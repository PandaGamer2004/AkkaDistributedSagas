package com.transactor.composition

import akka.actor.typed.ActorSystem
import com.typesafe.config.Config


object ModuleRegistrator{
    def registerToActorSystem[TChannel](
        actorSystem: ActorSystem[TChannel]
    ) = ???
}