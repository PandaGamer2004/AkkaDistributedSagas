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
import scala.language.implicitConversions
import scala.util.Try
import com.transactor.serialization.{SerDe, SerializedJson}
import com.transactor.protocols.SagaStepContract.SagaStepExecutionResult
import scala.util.Success
import scala.util.Failure
import com.transactor.configuration.conversions.ConfigurationConversions
import com.transactor.composition.{SagaCompositionRoot, StartProgram}



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

case class FailedToCompiledSagaPipeline(reason: String)

trait PivotalRegistration[TInitial, TStepPayload] {
    //Register error but don't infer type for it, as of now use only for logging
    def registerRunToCompletion[TStepResult <: Matchable, TErrorResult <: SagaStepFailure, TStepKey <: StepKeyBase](
        step: StepData[TStepPayload, TStepResult, TErrorResult, TStepKey]
    ): PivotalRegistration[TInitial, TStepResult]

    def compile(): Either[SagaPipeline[TInitial, TStepPayload], FailedToCompiledSagaPipeline]
}


//Template parametes is needed here to let scala deduct patterns in runtime
sealed trait SagaPipelineContract[TInputType, TResultType]

case class StartSagaPipeline[TInputType, TResultType](
        sagaInput: TInputType,
        sagaConfext: Option[String],
        //In case if operation was failed, it will return None or value
        notifyCompletion: ActorRef[Option[TResultType]]
) extends SagaPipelineContract[TInputType, TResultType]


case class SagaPipelineFailed[TInputType, TResultType](
    notifyResult: ActorRef[Option[TResultType]]
) extends SagaPipelineContract[TInputType, TResultType]

case class SagaPipelineCompleted[TInputType, TResultType](
    result: TResultType,
    notifyResult: ActorRef[Option[TResultType]]
) extends SagaPipelineContract[TInputType, TResultType] {}



trait SagaPipeline[TInputType, TResultType]{
    def makePipelineBehaviour: Behavior[StartSagaPipeline[TInputType, TResultType]]
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



case class StartInteractionExternal[TInitial, TStepPayload <: Matchable](
        sagaId: UUID,
        spinuip: TInitial,
        notifyResult: ActorRef[Option[TStepPayload]]
)

class SagaRegistration[TInitial, TStepPayload <: Matchable]
(  
    val mergedBehaviour: Option[Behavior[StartInteractionExternal[TInitial, TStepPayload]]]
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
        val startupBehaviour 
            = makeStartInternalInteractionBehaviour(step)

        val resultBehaviour = this.mergedBehaviour match {
            //On first iteration we always have TInitial = TStepPayload, so here we are
            //Will perform runtime casting
            case None => Behaviors.receive[StartInteractionExternal[TInitial, TStepResult]]{
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

            case Some(nestedMessageHandler) => Behaviors.receive[StartInteractionExternal[TInitial, TStepResult]]{
                (context, externalInteractionStartEvent) => {
                    val startInteractionExternalRef = context.spawnAnonymous(nestedMessageHandler)

                    val previousStepInteractionResult = 
                        context.spawnAnonymous(
                            Behaviors.receive[Option[TStepPayload]]{
                                (context, message) => {
                                    message match {
                                        case None => {
                                            //If we facing a failure then just propagate and leave and stop adapter actor
                                            externalInteractionStartEvent.notifyResult ! None
                                            Behaviors.stopped
                                        }
                                        case Some(value) => {
                                                
                                            val stepBehaviour = context.spawnAnonymous(startupBehaviour)


                                            val stepInteractionResultTranslator
                                                = makeStepInteractionResultTranslator[TStepResult, TStepKey](
                                                    externalInteractionStartEvent.notifyResult
                                                )

                                            val transalationActor = context.spawnAnonymous(
                                                stepInteractionResultTranslator
                                            )                                            

                                            stepBehaviour ! StartInteraction[TStepResult, TStepKey](
                                                sagaId = externalInteractionStartEvent.sagaId,
                                                interactionPaylod = value,
                                                notifyResult = transalationActor                                                
                                            )

                                            Behaviors.stopped
                                        }
                                    }
                                }
                            }
                        )

                    
                    startInteractionExternalRef ! StartInteractionExternal[TInitial, TStepPayload](
                        externalInteractionStartEvent.sagaId,
                        externalInteractionStartEvent.spinuip,
                        previousStepInteractionResult
                    )

                    Behaviors.ignore
                }
            }
    
        }

        new SagaRegistration[TInitial, TStepResult](Some(resultBehaviour))
    }
    

    private def makeStartInternalInteractionBehaviour[
        TStepResult <: Matchable, 
        TStepKey <: StepKeyBase,
        TErrorResult <: SagaStepFailure    
    ](
        sagaStep: StepDataWithCompensation[TStepPayload, TStepResult, TErrorResult, TStepKey]
    ) = createMdcForInteraction(sagaStep.descriptor){
                //Seting up behaviour that will be responssilbe for receiving mesage
                Behaviors.receive[StartInteraction[TStepResult, TStepKey]]{
                    (ctx, message) => {
                        message match {
                                case StartInteraction[TStepResult, TStepKey](sagaId, interactionPayload, notifyResult) => {
                                    val interactionHandlerName
                                        = makeGenerticStepActorName("StepInteractionHandler")(sagaId, sagaStep.descriptor) 

                                    val interactionHandler
                                        = ctx.spawn(
                                            interacationCoordinator(
                                                sagaStep, 
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


    private def makeStepInteractionResultTranslator[TStepResult <: Matchable, TStepKey <: StepKeyBase](
        externalMessageNotificator: ActorRef[Option[TStepResult]]
    ): Behavior[NotifyStepInteractionResult[TStepResult, TStepKey]]
        //Here by the way and before starting of start interaction internal could be implemented
        //Saga state stash
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
    


    override def compile(): Either[SagaPipeline[TInitial, TStepPayload], FailedToCompiledSagaPipeline] = {
        mergedBehaviour match {
            case Some(mergedSagaBehaviour) => Left(makeSagaPipeline(mergedSagaBehaviour))
            case None => Right(
                FailedToCompiledSagaPipeline(
                    reason = "Saga misconfigured: no steps were configured."
                    )
                )
        }
    }

    private def makeSagaPipeline(mergedSagaBehaviour: Behavior[StartInteractionExternal[TInitial, TStepPayload]]): SagaPipeline[TInitial, TStepPayload] = new:
        def makePipelineBehaviour
        : Behavior[StartSagaPipeline[TInitial, TStepPayload]] = Behaviors.receive[SagaPipelineContract[TInitial, TStepPayload]]{
            (context, pipelineStartMessage) => {
                pipelineStartMessage match {
                    case StartSagaPipeline(sagaInput, sagaConfext, notifyCompletion) => {
                        val sagaId = UUID.randomUUID()
                        val sagaContextName = sagaConfext.getOrElse("default")
                        context.log.info2(s"Saga {sagaId} have started, with context {context}", sagaId, sagaContextName)

                        val adapterResponseFromInteraction = context.messageAdapter[Option[TStepPayload]](
                            paylaod => paylaod match {
                                case Some(value) => SagaPipelineCompleted[TInitial, TStepPayload](value, notifyCompletion)
                                case None => SagaPipelineFailed(notifyCompletion)
                            }
                        )

                        val sagaMainBehaviour = context.spawn(
                            mergedSagaBehaviour, 
                            s"SagaPipelineActor${sagaId}_${context}"
                        )
                        sagaMainBehaviour ! StartInteractionExternal(
                            sagaId,
                            sagaInput,
                            adapterResponseFromInteraction
                        )
                        Behaviors.same
                    }
                    case SagaPipelineFailed(notifyResult) => {
                        notifyResult ! None
                        Behaviors.stopped
                    }
                    case SagaPipelineCompleted(result, notifyResult) => {
                        notifyResult ! Some(result)
                        Behaviors.stopped
                    }
        
                }
            }
        }.narrow
        
}

object SagaRoot {    
    def makeRegistration[TInitial <: Matchable](): ResettableRegistration[TInitial, TInitial] = 
        new SagaRegistration[TInitial, TInitial](None)

}




@main def main = {
    val configuredSampleSaga =  SagaCompositionRoot.sampleSaga
    val bootstrapActorSystem = ActorSystem(
        configuredSampleSaga, 
        "TestActorSystem"
    )


    bootstrapActorSystem ! StartProgram()

    bootstrapActorSystem.wait()
}

