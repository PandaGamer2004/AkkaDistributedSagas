package com.transactor.configuration.conversions

import com.transactor.configuration.models.SagaConfiguration
import akka.actor.typed.scaladsl.ActorContext
import scala.concurrent.duration._


object ConfigurationConversions {
    
    def getSagaConfiguration[TType](context: ActorContext[TType]): SagaConfiguration = new:
        def maxCompensationRetries: Int = 8

        def stepExecutionDeadline: FiniteDuration = 10.seconds
}   
