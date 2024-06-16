package com.transactor.composition

import akka.actor.typed.ActorSystem
import com.typesafe.config.Config
import akka.actor.typed.Behavior
import com.transactor.protocols.SagaStepFailure
import akka.actor.typed.scaladsl.Behaviors
import com.transactor.protocols.StepDescriptor
import com.transactor.protocols.SagaRoot
import com.transactor.protocols.SagaSteps.sagaStepWithCompensation
import com.transactor.protocols.SagaStepContract.PerformSagaStep
import com.transactor.protocols.SagaSteps.sagaStep
import akka.Done
import java.util.UUID
import com.transactor.protocols.StartSagaPipeline

case class Step1Result(result1: String)

case class StartProgram()

case class Step2Result(result2: String)

case class Step3Result(result3: String)

case class CustomError(message: String) extends SagaStepFailure

case class WrappedThrowable[TException <: Throwable](ex: TException) extends SagaStepFailure

object SagaCompositionRoot {


  def sampleSaga: Behavior[StartProgram] = Behaviors.receive { (context, msg) =>
    {
      val FirstStepDescriptor  = StepDescriptor("First step")
      val SecondStepDescriptor = StepDescriptor("Second step")
      val compiledSaga = SagaRoot
        .makeRegistration()
        .registerRessetable(
          sagaStepWithCompensation(
            stepInitialize =
              Behaviors.receive[PerformSagaStep[String, Step1Result, WrappedThrowable[Throwable]]] { (ctx, message) =>
                {
                  //Perform saga step and stop integration actor
                  message.notifyResult ! Left(Step1Result("Step 1 is completed"))
                  Behaviors.stopped
                }
              },
            stepDescriptor = FirstStepDescriptor,
            stepCompensation = Behaviors.unhandled,
            stepNotifyError = context.spawnAnonymous(Behaviors.receive { (context, message) =>
              {
                Behaviors.same
              }
            })
          )
        )
        .registerPivotal(
          sagaStepWithCompensation(
            stepInitialize = Behaviors.receive[PerformSagaStep[Step1Result, Step2Result, CustomError]] {
              (ctx, message) => Behaviors.same
            },
            stepDescriptor = SecondStepDescriptor,
            stepCompensation = Behaviors.unhandled,
            stepNotifyError = context.spawnAnonymous(Behaviors.receive { (context, message) =>
              Behaviors.same
            })
          )
        )
        .registerRunToCompletion(
          sagaStep(
            stepInitialize = Behaviors.receive[PerformSagaStep[Step2Result, Done, CustomError]] { (ctx, message) =>
              Behaviors.same
            },
            stepDescriptor = StepDescriptor("Last step"),
            //If we want to ingnore run to completion step just simply pass ignore step
            stepNotifyError = context.system.ignoreRef
          )
        )
        .compile()

      compiledSaga match {
        case Left(sagaPipeline) => {
          val executionId = UUID.randomUUID()
          val pipelineBehaviour = sagaPipeline.makePipelineBehaviour
          val pipelineActorRef = context.spawn(pipelineBehaviour, s"SagaExecutionPipeline${executionId}")

          
          val sagaCompletionBehavious = Behaviors.receiveMessage[Option[Done]]{
            message => message match {
              case Some(value) => {
                println("Saga execution completed")
                Behaviors.stopped
              }
              case None => {
                println("Saga execution failed")
                Behaviors.stopped
              }
            }
          }

          val sagaCompletionHandler = context.spawn(sagaCompletionBehavious, s"SagaCompletionHandler${executionId}")

          pipelineActorRef ! StartSagaPipeline(
            "There is my start message",
            Some(s"Execution${executionId}"),
            sagaCompletionHandler
          )

          Behaviors.same
    
        }
        case Right(value) => {
          context.log.error("Failed to create saga for the requested pipeline")
          Behaviors.stopped
        }
      }

      Behaviors.same
    }
  }

}
