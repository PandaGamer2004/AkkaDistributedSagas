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
import com.transactor.protocols.SagaSteps.sagaStepWithCompensation
import com.transactor.protocols.SagaSteps.sagaStep
import akka.NotUsed
import scala.concurrent.java8.FuturesConvertersImpl.P
import upickle.default
import scala.language.implicitConversions
import scala.util.Try
import com.transactor.serialization.{SerDe, SerializedJson}
import com.transactor.protocols.SagaStepContract.SagaStepExecutionResult
import scala.util.Success
import scala.util.Failure
import com.transactor.configuration.conversions.ConfigurationConversions
import com.transactor.composition.ModuleRegistrator.registerToActorSystem



type StepKeyBase = String & Singleton
case class StepDescriptor[+TStepKey <: StepKeyBase](stepName: TStepKey)



case class ErrorNotification[TKey <: StepKeyBase, TError](
    errorKey: StepDescriptor[TKey], 
    error: TError
 )

trait StepData[TStepPayload, TSuccessResult <: Matchable, TErrorResult <: SagaStepFailure, TStepKey <: StepKeyBase]{
    def initialize: Behavior[SagaStepContract.PerformSagaStep[TStepPayload, TSuccessResult, TErrorResult]]
    def notifyError: ActorRef[ErrorDescription[TStepKey, TErrorResult]]
    def descriptor: StepDescriptor[TStepKey]
}


object CompesationContract{
    
    case class CompensationFinished[TStepKey <: StepKeyBase](
        stepDescriptor: StepDescriptor[TStepKey]
    )

    case class CompensationRequest[TStepPayload, TErrorResult, TStepKey <: StepKeyBase](
        stepInitializedWith: TStepPayload,
        error: TErrorResult,
        stepDescriptor: StepDescriptor[TStepKey],
        replyCompleteCompenastion: ActorRef[StatusReply[CompensationFinished[TStepKey]]]
    )
}



trait StepDataWithCompensation[TStepPayload, TSuccessResult <: Matchable, TErrorResult <: SagaStepFailure, TStepKey <: StepKeyBase] 
    extends StepData[TStepPayload, TSuccessResult, TErrorResult, TStepKey] 
{
    def compensation: Behavior[CompesationContract.CompensationRequest[TStepPayload, TErrorResult, TStepKey]]
}


object SagaSteps{
    def sagaStep[TStepPayload, TSuccessResult <: Matchable, TErrorResult <: SagaStepFailure, TStepKey <: StepKeyBase](
        stepInitialize: Behavior[PerformSagaStep[TStepPayload, TSuccessResult, TErrorResult]],
        stepDescriptor: StepDescriptor[TStepKey],
        stepNotifyError: ActorRef[ErrorDescription[TStepKey, TErrorResult]] 
    ): StepData[TStepPayload, TSuccessResult, TErrorResult, TStepKey] = new:
        def initialize = stepInitialize
        def descriptor: StepDescriptor[TStepKey] = stepDescriptor

        def notifyError: ActorRef[ErrorDescription[TStepKey, TErrorResult]] = stepNotifyError


    def sagaStepWithCompensation[TStepPayload, TSuccessResult <: Matchable, TErrorResult <: SagaStepFailure, TStepKey <: StepKeyBase](
        stepInitialize: Behavior[PerformSagaStep[TStepPayload, TSuccessResult, TErrorResult]],
        stepDescriptor: StepDescriptor[TStepKey],
        stepCompensation: Behavior[CompensationRequest[TStepPayload, TErrorResult, TStepKey]],
        stepNotifyError: ActorRef[ErrorDescription[TStepKey, TErrorResult]] 
    ): StepDataWithCompensation[TStepPayload, TSuccessResult, TErrorResult, TStepKey] = new:
        def initialize = stepInitialize
        def descriptor = stepDescriptor
        def compensation = stepCompensation
    
        def notifyError: ActorRef[ErrorDescription[TStepKey, TErrorResult]] = stepNotifyError
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




trait ResettableRegistration[TInitial, TStepPayload] {
    def registerRessetable[TStepResult <: Matchable, TErrorResult2 <: SagaStepFailure, TStepKey <: StepKeyBase](
        step: StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult2, TStepKey],
    ): ResettableRegistration[TInitial, TStepResult]

    def registerPivotal[TStepResult <: Matchable, TErrorResult2 <: SagaStepFailure, TStepKey <: StepKeyBase](
        step: StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult2, TStepKey],
    ): PivotalRegistration[TInitial, TStepResult]
}


trait PivotalRegistration[TInitial, TStepPayload] {
    //Register error but don't infer type for it, as of now use only for logging
    def registerRunToCompletion[TStepResult <: Matchable, TErrorResult <: SagaStepFailure, TStepKey <: StepKeyBase](
        step: StepData[TStepPayload, TStepResult, TErrorResult, TStepKey]
    ): PivotalRegistration[TInitial, TStepResult]

    def compile(context: ActorContext[?]): SagaPipeline[TInitial, TStepPayload]
}





trait SagaPipeline[TInputType, TResultType]{
    def execute(
        input: TInputType,
        notifyCompletion: ActorRef[TResultType]
    ): ActorRef[Nothing]
}


object SagaStepContract {

    type SagaStepExecutionResult[TSuccessPayload, TErrorResult <: Matchable]
         = Either[TSuccessPayload, TErrorResult]

    
    // Base class for performing a saga step
    case class PerformSagaStep[TInitializePayload, TSucessResult, TErrorResult <: Matchable](
        stepPayload: TInitializePayload,
        exeuctionDeadline: FiniteDuration,
        notifyResult: ActorRef[SagaStepExecutionResult[TSucessResult, TErrorResult]]
    )
}



case class StartInteractionExtrnal[TInitial, TStepPayload <: Matchable](
        sagaId: UUID,
        spinuip: TInitial,
        notifyResult: ActorRef[Option[TStepPayload]]
)

class SagaRegistration[TInitial, TStepPayload <: Matchable]
(  
    val mergedBehaviour: Option[Behavior[StartInteractionExtrnal[TInitial, TStepPayload]]]
)extends 
     ResettableRegistration[TInitial, TStepPayload],
     PivotalRegistration[TInitial, TStepPayload]
{

    case object StepFailed{}

    case class StepResult[TPayload, TStepKey <: StepKeyBase](
        payload: TPayload,
        descriptor: StepDescriptor[TStepKey]
    )


    //Registered steps keys here 
    type NotifyStepInteractionResult[TStepResult, TStepKey <: StepKeyBase] 
        = Either[StepResult[TStepResult, TStepKey], StepResult[StepFailed.type, TStepKey]]


    

    private case class StartInteraction[TStepResult <: Matchable, TStepKey <: StepKeyBase](
        sagaId: UUID,
        interactionPaylod: TStepPayload,
        notifyResult: ActorRef[NotifyStepInteractionResult[TStepResult, TStepKey]]
    )



    sealed trait InteractionInternalContract[
        TStepResult <: Matchable, 
        TErrorResult <: SagaStepFailure,
        TStepKey <: StepKeyBase
    ] 


    private case class CompensationFinished[
        TStepResult <: Matchable, 
        TErrorResult <: SagaStepFailure, 
        TStepKey <: StepKeyBase
    ](
        sagaId: UUID,
        sagaErrorResult: TErrorResult,
        notifyResult: ActorRef[NotifyStepInteractionResult[TStepResult, TStepKey]]
        
    ) extends InteractionInternalContract[TStepResult, TErrorResult, TStepKey]



    private case class InteractionCompleted[
        TStepResult <: Matchable, 
        TErrorResult <: SagaStepFailure, 
        TStepKey <: StepKeyBase
    ](
        successResult: TStepResult,
        sagaId: UUID,
        notifyResult: ActorRef[NotifyStepInteractionResult[TStepResult, TStepKey]]
    ) extends InteractionInternalContract[TStepResult, TErrorResult, TStepKey]

    private case class InteractionFailed[
        TStepResult <: Matchable, 
        TErrorResult <: SagaStepFailure, 
        TStepKey <: StepKeyBase
    ](
        errorResult: TErrorResult,
        sagaId: UUID,
        notifyResult: ActorRef[NotifyStepInteractionResult[TStepResult, TStepKey]]
    ) extends InteractionInternalContract[TStepResult, TErrorResult, TStepKey]



    private def registerRessetableInternal[
        TStepResult <: Matchable, 
        TErrorResult <: SagaStepFailure, 
        TStepKey <: StepKeyBase
    ](step: StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult, TStepKey]): SagaRegistration[TInitial, TStepResult] = {


        val startupBehaviour = createMdcForInteraction(step.descriptor){
                //Seting up behaviour that will be responssilbe for receiving mesage
                Behaviors.receive[StartInteraction[TStepResult, TStepKey]]{
                    (ctx, message) => {
                        message match {
                                case StartInteraction[TStepResult, TStepKey](sagaId, interactionPayload, notifyResult) => {
                                    val interactionHandlerName
                                        = makeGenerticStepActorName("StepInteractionHandler")(sagaId, step.descriptor) 

                                    val interactionHandler
                                        = ctx.spawn(
                                            interacationCoordinator(
                                                step, 
                                                sagaId, 
                                                notifyResult, 
                                                interactionPayload
                                            ),
                                            interactionHandlerName
                                        )

                                    Behaviors.same
                                }                            
                        }   
                    }
                }
        }


        this.mergedBehaviour match {
            //On first iteration we always have TInitial = TStepPayload, so here we are
            //Will perform runtime casting
            case None => Behaviors.receive[StartInteractionExtrnal[TInitial, TStepResult]]{
                (context, externalStartupMessage) => {
                    val spawnedLocalOperationContract = context.spawnAnonymous(startupBehaviour)
                    
                    val errorMessageAdapterBehaviour 
                            = makeStepInteractionResultTranslator[TStepResult, TStepKey](
                                externalStartupMessage.notifyResult
                            )


                    val errorMessageAdapterActor = context.spawnAnonymous(errorMessageAdapterBehaviour)
                    
                    spawnedLocalOperationContract ! StartInteraction[TStepResult, TStepKey](
                        sagaId = externalStartupMessage.sagaId,
                        interactionPaylod = externalStartupMessage.spinuip.asInstanceOf[TStepPayload],
                        notifyResult = errorMessageAdapterActor
                    )

                    Behaviors.ignore
                }
            }

            case Some(nestedMessageHandler) => Behaviors.receive[StartInteractionExtrnal[TInitial, TStepPayload]]{
                (context, externalInteractionStartEvent) => {
                    val startInteractionExternalRef = context.spawnAnonymous(nestedMessageHandler)

                    val previousStepInteractionResult = 
                        context.spawnAnonymous(
                            Behaviors.receive[Option[TStepPayload]]{
                                (context, message) => {
                                    message match {
                                        case None => {
                                            //Just propagate faiure
                                            externalInteractionStartEvent.notifyResult ! None
                                            Behaviors.stopped
                                        }
                                        case Some(value) => {
                                                

                                            val stepBehaviour = context.spawnAnonymous(startupBehaviour)
                                            val stepInteractionResultTranslator
                                                = makeStepInteractionResultTranslator[TStepPayload, TStepKey](
                                                    externalInteractionStartEvent.notifyResult
                                                )
                                            
                                            
                                            stepBehaviour ! StartInteraction[TStepResult, TStepKey](
                                                sagaId = externalInteractionStartEvent.sagaId,
                                                interactionPaylod = value,
                                                notifyResult = null                                                
                                            )


                                            Behaviors.stopped
                                        }
                                    }
                                }
                            }
                        )

                    
                    startInteractionExternalRef ! StartInteractionExtrnal[TInitial, TStepPayload](
                        externalInteractionStartEvent.sagaId,
                        externalInteractionStartEvent.spinuip,
                        previousStepInteractionResult
                    )

                    Behaviors.ignore
                }
            }
    
        }

        throw new Exception("Error")
    }
    

    private def makeStepInteractionResultTranslator[TStepResult <: Matchable, TStepKey <: StepKeyBase](
        externalMessageNotificator: ActorRef[Option[TStepResult]]
    ): Behavior[NotifyStepInteractionResult[TStepResult, TStepKey]]
        = Behaviors.receiveMessage[NotifyStepInteractionResult[TStepResult, TStepKey]]{
            message => {
                message match {
                    case Left(value) => {
                        externalMessageNotificator ! Some(value.payload)
                        Behaviors.stopped
                    }
                    case Right(value) => {
                        externalMessageNotificator ! None
                        Behaviors.stopped
                    }
                }
            }
        }


    override def registerRessetable[
        TStepResult <: Matchable, 
        TErrorResult <: SagaStepFailure, 
        TStepKey <: StepKeyBase
    ](
        step: StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult, TStepKey]
    ): ResettableRegistration[TInitial, TStepResult]
         = this.registerRessetableInternal(step)



    private def interacationCoordinator[
        TStepResult <: Matchable, 
        TErrorResult <: SagaStepFailure,
        TStepKey <: StepKeyBase
    ](
        step: StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult, TStepKey],
        sagaId: UUID,
        notifyResult: ActorRef[NotifyStepInteractionResult[TStepResult, TStepKey]],
        interactionPayload: TStepPayload,
    ): Behavior[InteractionInternalContract[TStepResult, TErrorResult, TStepKey]]
        = Behaviors.setup[InteractionInternalContract[TStepResult, TErrorResult, TStepKey]]{
            context => {

                val sagaConfiguration = 
                        ConfigurationConversions.getSagaConfiguration(context)
                
                val stepExecutorName = makeGenerticStepActorName("StepExecutor")(sagaId, step.descriptor)
                            
                val spawnedInteractionActor 
                                = context.spawn(step.initialize, stepExecutorName)

                val pipedInteraction = context.messageAdapter[SagaStepExecutionResult[TStepResult, TErrorResult]](
                    sagaExecutionResult => sagaExecutionResult match {
                        case Left(result) => InteractionCompleted(result, sagaId, notifyResult)
                        case Right(error) => InteractionFailed(error, sagaId, notifyResult)
                    }
                )
                
                spawnedInteractionActor ! PerformSagaStep[TStepPayload, TStepResult, TErrorResult](
                                exeuctionDeadline = sagaConfiguration.stepExecutionDeadline,
                                notifyResult = pipedInteraction,
                                stepPayload = interactionPayload
                )

                

                Behaviors.receiveMessage[InteractionInternalContract[TStepResult, TErrorResult, TStepKey]](
                    (message) => message match {
                        case InteractionCompleted[TStepResult, TErrorResult, TStepKey](successResult, sagaId, notifyResult) => {
                            //Notify that interaction completed successfully on stop interaction
                            val sucessStepResult = StepResult[TStepResult, TStepKey](
                                                        payload = successResult,
                                                        descriptor = step.descriptor
                                                    )
                            notifyResult ! Left(sucessStepResult)
                            //Stop interaction when completed
                            Behaviors.stopped
                        }

                        case InteractionFailed[TStepResult, TErrorResult, TStepKey](errorResult, sagaId, notifyResult) => {

                            // if(Behavior.isUnhandled(step.compensation)){
                            //     //Means we could skip compensation stege because data
                            //     //That is supplied by compenstaion shouldn't be used
                            // }
                            // Todo implement compensations logic
                            // val compenastionActor = context.spawn(
                            //     step.compensation,
                            //     makeGenerticStepActorName("StepCompensation")(sagaId, step.descriptor)
                            // )

                            val failedStepResult = StepResult[StepFailed.type, TStepKey](
                                StepFailed,
                                descriptor = step.descriptor
                            )

                            notifyResult ! Right(failedStepResult)  
                            Behaviors.stopped
                        }
                        case _ => throw new Exception()
                    }
                )
            }
        }


    

            
        


    
    private def makeGenerticStepActorName[TStepKey <: StepKeyBase](stepType: String)(sagaId: UUID, descriptor: StepDescriptor[TStepKey])
        = s"${stepType}_${sagaId}-${descriptor.stepName}"

    private def createMdcForInteraction[TKey <: StepKeyBase, TStepResult <: Matchable](step: StepDescriptor[TKey]) = {
        Behaviors.withMdc[StartInteraction[TStepResult, TKey]](
            Map("Step key" -> step.stepName),
            interaction => Map("SagaId" -> interaction.sagaId.toString)
        )
    }



    

    override def registerRunToCompletion[TStepResult <: Matchable, TErrorResult <: SagaStepFailure, TStepKey <: StepKeyBase](
        step: StepData[TStepPayload, TStepResult, TErrorResult, TStepKey]
    ): PivotalRegistration[TInitial, TStepResult] = {
        //Basically adapter pattern, it will unstruct that compensation is basically unhnalded 
        val adaptedSagaStep = new StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult, TStepKey]{
            def compensation: Behavior[CompensationRequest[TStepPayload, TErrorResult, TStepKey]]
                 = Behaviors.unhandled
            def descriptor: StepDescriptor[TStepKey] 
                = step.descriptor

            def initialize: Behavior[PerformSagaStep[TStepPayload, TStepResult, TErrorResult]]
                = step.initialize

            def notifyError: ActorRef[ErrorDescription[TStepKey, TErrorResult]] 
                = step.notifyError
        }

        this.registerRunToCompletion(adaptedSagaStep)
    }


    override def registerPivotal[TStepResult <: Matchable, TErrorResult2 <: SagaStepFailure, TStepKey <: StepKeyBase](
        step: StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult2, TStepKey]
    ): PivotalRegistration[TInitial, TStepResult] = 
        this.registerRessetableInternal(step)
    




    sealed trait CoordinatorContract
    
    sealed trait CoordinatorRequest extends CoordinatorContract
    sealed trait CoordinatorResponse extends CoordinatorContract

    case class RegisterNotifications(
        notifyCompleted: ActorRef[TStepPayload],
    ) extends CoordinatorContract


    override def compile(context: ActorContext[?]): SagaPipeline[TInitial, TStepPayload] = new:
        override def execute(
            input: TInitial, 
            notifyCompletion: ActorRef[TStepPayload]
        ): ActorRef[Nothing] = {

            val sagaId = UUID.randomUUID();
            val tranactionCoordinatorRef 
                = context.spawn(
                    makeTransactionCoordinator(sagaId),
                    s"TransactionCoordinator${sagaId}"
                )

            tranactionCoordinatorRef ! RegisterNotifications(
                notifyCompletion,
            )

            tranactionCoordinatorRef

        }

    def makeTransactionCoordinator[TInitial](withId: UUID): Behavior[CoordinatorContract] = ???
        
}

object SagaRoot {    
    def makeRegistration[TInitial <: Matchable](): ResettableRegistration[TInitial, TInitial] = 
        new SagaRegistration[TInitial, TInitial](None)

}





case class Step1Result(result1: String)

case class Step2Result(result2: String)

case class Step3Result(result3: String)

case class CustomError(message: String) extends SagaStepFailure

case class WrappedThrowable[TException <: Throwable](ex: TException) extends SagaStepFailure



case class StartProgram()

object SagaCompositionRoot{
    def apply(): Behavior[StartProgram] = Behaviors.receive{
        (context, msg) => {
            val FirstStepDescriptor = StepDescriptor("First step")
            val SecondStepDescriptor = StepDescriptor("Second step")
            val compiledSaga = SagaRoot
                    .makeRegistration()
                    .registerRessetable(
                        sagaStepWithCompensation(
                            stepInitialize = Behaviors.receive[PerformSagaStep[String, Step1Result, WrappedThrowable[Throwable]]]{
                                (ctx, message) => {
                                    //Perform saga step and stop integration actor
                                    message.notifyResult ! Left(Step1Result("Step 1 is completed"))
                                    Behaviors.stopped
                                }
                            },
                            stepDescriptor = FirstStepDescriptor,
                            stepCompensation = Behaviors.unhandled,
                            stepNotifyError = context.spawnAnonymous(Behaviors.receive{
                                (context, message) => {
                                    Behaviors.same
                                }
                            })
                        )
                    )
                    .registerPivotal(
                        sagaStepWithCompensation(
                            stepInitialize = Behaviors.receive[PerformSagaStep[Step1Result, Step2Result, CustomError]]{
                                (ctx, message) => Behaviors.same
                            },
                            stepDescriptor = SecondStepDescriptor,
                            stepCompensation = Behaviors.unhandled,
                            stepNotifyError = context.spawnAnonymous(Behaviors.receive{
                                (context, message) => Behaviors.same
                            })
                        )
                    )
                    .registerRunToCompletion(
                        sagaStep(
                            stepInitialize = Behaviors.receive[PerformSagaStep[Step2Result, Done, CustomError]]{
                                (ctx, message) => Behaviors.same 
                            },
                            stepDescriptor = StepDescriptor("Last step"),
                            //If we want to ingnore run to completion step just simply pass ignore step
                            stepNotifyError = context.system.ignoreRef
                        )
                    )
                    .compile(context)

            
            compiledSaga.execute(
                    input = "Sample input",
                    notifyCompletion = context.spawnAnonymous(Behaviors.receive {
                        (ctx, messsag) => Behaviors.same
                    })
                )

            Behaviors.same   
        }
    }

}


@main def main = {
    val bootstrapActorSystem = ActorSystem(
        SagaCompositionRoot(), 
        "TestActorSystem"
    )


    bootstrapActorSystem ! StartProgram()

    bootstrapActorSystem.wait()
}

