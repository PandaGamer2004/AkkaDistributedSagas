package com.transactor


package protocols

import akka.actor.typed._
import akka.actor.typed.scaladsl._

import scala.concurrent.duration._
import scala.compiletime.ops.string
import com.typesafe.config.Config
import akka.pattern.StatusReply
import java.util.UUID
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.receptionist.Receptionist
import com.transactor.protocols.SagaStepContract.PerformSagaStep
import akka.actor.ProviderSelection.Custom
import com.transactor.protocols.CompesationContract.CompensationRequest
import akka.Done
import scala.annotation.tailrec





object SagaStepContract{
    sealed trait Request
    sealed trait Response

    // Base class for performing a saga step
    case class PerformSagaStep[TInitializePayload, TErrorResult <: Matchable, TSucessResult](
        sagaStep: TInitializePayload,
        executionDeadline: FiniteDuration
    ) extends Request


    case class SagaSucceeded[TResultPayload](successPayload: TResultPayload) extends Response

    case class SagaStepFailed[TFailureResponse](failurePayload: TFailureResponse) extends Response
    
    case object SagaStepDeadlineExceeded
}



case class ErrorNotification[TKey <: StepKeyBase, TError](
    errorKey: StepDescriptor[TKey], 
    error: TError
 )


case class SagaPipelineError[TError](stepKey: String, error: TError)
trait SagaPipeline[TInputType, TResultType, TErrorType]{
    def execute(
        input: TInputType,
        notifyError: Behavior[TErrorType], 
        notifyCompletion: Behavior[TResultType]
    ): Unit
}




type StepKeyBase = String & Singleton

case class StepDescriptor[TStepKey <: StepKeyBase](stepName: TStepKey)


trait StepData[TStepPayload, TSuccessResult <: Matchable, TErrorResult, TStepKey <: StepKeyBase]{
    def initialize: Behavior[SagaStepContract.PerformSagaStep[TStepPayload, TSuccessResult, TErrorResult]]
    def descriptor: StepDescriptor[TStepKey]
}


object CompesationContract{
    
    case class CompensationResponse[TStepKey <: StepKeyBase](
        stepDescriptor: StepDescriptor[TStepKey]
    )

    case class CompensationRequest[TStepPayload, TErrorResult, TStepKey <: StepKeyBase](
        stepInitializedWith: TStepPayload,
        error: TErrorResult,
        stepDescriptor: StepDescriptor[TStepKey],
        replyCompleteCompenastion: ActorRef[StatusReply[CompensationResponse[TStepKey]]]
    )
}



trait StepDataWithCompensation[TStepPayload, TSuccessResult <: Matchable, TErrorResult, TStepKey <: StepKeyBase] 
    extends StepData[TStepPayload, TSuccessResult, TErrorResult, TStepKey] 
{
    def compensation: Behavior[CompesationContract.CompensationRequest[TStepPayload, TErrorResult, TStepKey]]
}

object SagaSteps{
    def sagaStep[TStepPayload, TSuccessResult <: Matchable, TErrorResult, TStepKey <: StepKeyBase](
        stepInitialize: Behavior[PerformSagaStep[TStepPayload, TSuccessResult, TErrorResult]],
        stepDescriptor: StepDescriptor[TStepKey]
    ): StepData[TStepPayload, TSuccessResult, TErrorResult, TStepKey] = new:
        def initialize = stepInitialize
        def descriptor: StepDescriptor[TStepKey] = stepDescriptor

    def sagaStepWithCompensation[TStepPayload, TSuccessResult <: Matchable, TErrorResult, TStepKey <: StepKeyBase](
        stepInitialize: Behavior[PerformSagaStep[TStepPayload, TSuccessResult, TErrorResult]],
        stepDescriptor: StepDescriptor[TStepKey],
        stepCompensation: Behavior[CompensationRequest[TStepPayload, TErrorResult, TStepKey]]
    ): StepDataWithCompensation[TStepPayload, TSuccessResult, TErrorResult, TStepKey] = new:
        def initialize = stepInitialize
        def descriptor = stepDescriptor
        def compensation = stepCompensation
}


//Required to make sure that all error types are Matcheable, because exception
trait SagaStepFailure

case class ErrorDescription[TStepKey <: StepKeyBase, TErrorResult <: SagaStepFailure](
    stepDescriptor: StepDescriptor[TStepKey],
    error: TErrorResult
)



//Because we are glui
type CombineError[TCombinator, TError <: SagaStepFailure, TStepKey <: StepKeyBase] 
    = ErrorDescription[TStepKey, TError] | TCombinator




trait ResettableRegistration[TInitial, TStepPayload, TErrorResult] {
    def registerRessetable[TStepResult <: Matchable, TErrorResult2 <: SagaStepFailure, TStepKey <: StepKeyBase](
        step: StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult2, TStepKey],
    ): ResettableRegistration[TInitial, TStepResult, CombineError[TErrorResult, TErrorResult2, TStepKey]]

    def registerPivotal[TStepResult <: Matchable, TErrorResult2 <: SagaStepFailure, TStepKey <: StepKeyBase](
        step: StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult2, TStepKey],
    ): PivotalRegistration[TInitial, TStepResult, CombineError[TErrorResult, TErrorResult2, TStepKey]]
}


trait PivotalRegistration[TInitial, TStepPayload, TErrorResult] {
    //Register error but don't infer type for it, as of now use only for logging
    def registerRunToCompletion[TStepResult <: Matchable, TErrorResult2, TStepKey <: StepKeyBase](
        step: StepData[TStepPayload, TStepResult, TErrorResult2, TStepKey]
    ): PivotalRegistration[TInitial, TStepResult, TErrorResult]

    def compile(): SagaPipeline[TInitial, TStepPayload, TErrorResult]
}


object SagaRoot {
    def makeRegistration[TInitial]()
            : ResettableRegistration[TInitial, TInitial, Nothing] = ???

}





case class Step1Result(result1: String)

case class Step2Result(result2: String)

case class Step3Result(result3: String)

case class CustomError(message: String) extends SagaStepFailure

case class WrappedThrowable[TException <: Throwable](ex: TException) extends SagaStepFailure
@main def main = {
    import SagaSteps._
    
    val FirstStepDescriptor = StepDescriptor("First step")
    val SecondStepDescriptor = StepDescriptor("Second step")

    val compiledSaga = 
        SagaRoot
        .makeRegistration()
        .registerRessetable(
            sagaStepWithCompensation(
                stepInitialize = Behaviors.receive[PerformSagaStep[String, Step1Result,WrappedThrowable[Throwable]]]{
                    (ctx, message) => Behaviors.same
                },
                stepDescriptor = FirstStepDescriptor,
                stepCompensation = Behaviors.unhandled
            )
        )
         .registerPivotal(
            sagaStepWithCompensation(
                stepInitialize = Behaviors.receive[PerformSagaStep[Step1Result, Step2Result, CustomError]]{
                    (ctx, message) => Behaviors.same
                },
                stepDescriptor = SecondStepDescriptor,
                stepCompensation = Behaviors.unhandled
            )
        )
        .registerRunToCompletion(
            sagaStep(
                stepInitialize = Behaviors.receive[PerformSagaStep[Step2Result, Done, CustomError]]{
                    (ctx, message) => Behaviors.same 
                },
                stepDescriptor = StepDescriptor("Last step")
            )
        ).compile()

    compiledSaga.execute(
        input = "Sample input",
        notifyCompletion = Behaviors.receive {
            (ctx, messsag) => Behaviors.same
        },
        notifyError = Behaviors.receiveMessage{
            message => {
                
                val res = message match {
                    case (ErrorDescription(FirstStepDescriptor, err@WrappedThrowable(ex))) => {
                        //Handle first error
                    }
                    case (ErrorDescription(SecondStepDescriptor, err@CustomError )) => {
                        //Handle second error
                    }
                }
                Behaviors.same
            }
        }
    )
}

