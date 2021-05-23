package info.jksystems.demo

import com.fasterxml.jackson.databind.ObjectMapper
import io.quarkus.runtime.StartupEvent
import io.vertx.core.Vertx
import org.jboss.logging.Logger
import org.springframework.cloud.gcp.pubsub.core.subscriber.PubSubSubscriberTemplate
import org.springframework.cloud.gcp.pubsub.reactive.PubSubReactiveFactory
import org.springframework.cloud.gcp.pubsub.support.DefaultSubscriberFactory
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.event.Observes

@ApplicationScoped
class PubSubReceiver(
    val vertx: Vertx,
    val objectMapper: ObjectMapper,
    val log: Logger
) {

    fun init(@Observes startupEvent: StartupEvent){
        val projectId = "project-id" // TODO: replace to specific projectId
        val subscriptionName = "subscription-name" // TODO: replace to specific subscription

        val defaultSubscriberFactory = DefaultSubscriberFactory { projectId }
        val template = PubSubSubscriberTemplate(defaultSubscriberFactory)
        val pubSubReactiveFactory = PubSubReactiveFactory(template, Schedulers.fromExecutorService(vertx.nettyEventLoopGroup()))
        pubSubReactiveFactory
            .poll (subscriptionName, 1000)
            .flatMap ({ msg ->
                val pubsubMessage = msg.pubsubMessage
                Mono.fromCallable { objectMapper.readValue( msg.pubsubMessage.data.toStringUtf8(), String::class.java) }
                    .flatMap {
                        log.info("Incoming Payload $it, messageId ${pubsubMessage.messageId}")
                        Mono.just(it)
                    }
                    .then(Mono.fromFuture {
                        log.info("Message ack on messageId ${pubsubMessage.messageId}")
                        msg.ack().completable()
                    })
                    .onErrorResume{ e ->
                        log.error("Message nacked on messageId ${msg.pubsubMessage.messageId}", e)
                        Mono.fromFuture { msg.nack().completable() }
                    }
            }, 4)
            .doOnSubscribe { log.info("Subscribing to topic!") }
            .subscribe()
    }
}