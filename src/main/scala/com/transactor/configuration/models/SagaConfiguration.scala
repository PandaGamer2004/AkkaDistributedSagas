package com.transactor.configuration.models

import scala.concurrent.duration.FiniteDuration

trait SagaConfiguration {
    def stepExecutionDeadline: FiniteDuration;
    
    def maxCompensationRetries: Int;
}
