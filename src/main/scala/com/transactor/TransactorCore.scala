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




object SagaStepContract{
    sealed trait Request
    sealed trait Response

    // Base class for performing a saga step
    case class PerformSagaStep[TInitializePayload, TErrorResult, TSucessResult](
        sagaStep: TInitializePayload,
        executionDeadline: FiniteDuration
    ) extends Request


    case class SagaSucceeded[TResultPayload](successPayload: TResultPayload) extends Response

    case class SagaStepFailed[TFailureResponse](failurePayload: TFailureResponse) extends Response
    
    case object SagaStepDeadlineExceeded
}




case class ErrorNotification[TKey <: String, TError](errorKey: TKey, error: TError)

case class CustomError(message: String)

type Alias = ErrorNotification["First step", String] | ErrorNotification["Second step", Throwable] | ErrorNotification["Thrid step", CustomError]

def handleError(notification: Alias): Unit = {
    notification match {
      case ErrorNotification("First step", error: String) =>
        println(s"First step failed: $error")
      case ErrorNotification("Second step", error: Throwable) =>
        println(s"Second step failed: ${error.getMessage}")
      case ErrorNotification("Third step", error: CustomError) =>
        println(s"Third step failed: ${error.message}")
      case _ =>
        println("Unknown error")
    }
  }

case class SagaPipelineError[TError](stepKey: String, error: TError)
trait SagaPipeline[TInputType, TErrorType, TResultType]{
    def execute(
        input: TInputType,
        notifyError: Behavior[TErrorType], 
        notifyCompletion: Behavior[TResultType]
    ): Unit
}


//This is interanl api to notify saga cordinator
case class StepData[TStepPayload, TSuccessResult, TErrorResult](
    initialize: Behavior[SagaStepContract.PerformSagaStep[TStepPayload, TSuccessResult, TErrorResult]],
    //Should be left as inetral api forever
    //notifyResult: ActorRef[SagaStepContract.SagaSucceeded[TSuccessResult] | SagaStepContract.SagaStepFailed[TErrorResult]], 
    stepKey: String
)





trait ResettableRegistration[TInitial, TStepPayload, TErrorResult] {
    def registerRessetable[TStepResult, TErrorResult2](
        step: StepData[TStepPayload, TStepResult, TErrorResult2]
    ): ResettableRegistration[TInitial, TStepResult, TErrorResult | TErrorResult2]

    def registerPivotal[TStepResult, TErrorResult2](
        step: StepData[TStepPayload, TStepResult, TErrorResult2]
    ): PivotalRegistration[TInitial, TStepResult, TErrorResult | TErrorResult2]
}

case object NotValuableError{}


trait PivotalRegistration[TInitial, TStepPayload, TErrorResult] {
    //Register error but don't infer type for it, as of now use only for logging
    def registerRunToCompletion[TStepResult, TErrorResult2](step: StepData[TStepPayload, TStepResult, TErrorResult2])
        : PivotalRegistration[TInitial, TStepResult, TErrorResult]

    def compile(): SagaPipeline[TInitial, TStepPayload, TErrorResult]
}




object SagaRoot {

    def withRessetableStep[TInitial, TStepPayload, TErrorResult](
        step: StepData[TInitial, TStepPayload, TErrorResult]
    ): ResettableRegistration[TInitial, TStepPayload, TErrorResult] = ???
    
    def withPivotalStep[TInitial, TStepPayload, TErrorResult](
        step: StepData[TInitial, TStepPayload, TErrorResult]
    ): PivotalRegistration[TInitial, TStepPayload, TErrorResult] = ???

}




case class Step1Result(result1: String)

case class Step2Result(result2: String)

case class Step3Result(result3: String)



@main def main = {
    val compiledSaga = SagaRoot.withRessetableStep(StepData(
        Behaviors.receive[PerformSagaStep[String, Step1Result, String]]{
            (ctx, message) => Behaviors.same
        },
        "Same message step"
    )).registerRessetable(
        StepData(
            Behaviors.receive[PerformSagaStep[Step1Result, Step2Result, Throwable]]{
                (ctx, message) => Behaviors.same
            },
            "Another step"
        )
    ).registerPivotal(
        StepData(
           Behaviors.receive[PerformSagaStep[Step2Result, Step3Result, CustomError]]{
                (ctx, message) => Behaviors.same
            },
            "Another step" 
        )
    ).compile()

    compiledSaga.execute(
        "Some input", 
        Behaviors.receiveMessage{
            (message) => {
                println(message)
                Behaviors.same
            }
        },
        Behaviors.receiveMessage{
            (message) => {
                message match {
                    case s: String => {
                        println("First step failed")
                    }
                    case t: Throwable => {
                        println("Second step failed")
                    }
                    case e: CustomError => {
                        println("Third step failed")
                    }
                    case _ => {
                        println("Unknown message type")
                    }
                    }
                Behaviors.same
            }
        }
    )
}

