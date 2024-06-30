package com.akoolla

import io.quarkus.vertx.ConsumeEvent
import io.smallrye.mutiny.Uni
import io.vertx.core.eventbus.EventBus
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import org.eclipse.microprofile.reactive.messaging.Channel
import org.eclipse.microprofile.reactive.messaging.Emitter
import org.eclipse.microprofile.reactive.messaging.Incoming
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

@ApplicationScoped
@Path("/hello")
class GreetingResource(
    @Channel("exception")
    private val emitter: Emitter<String>,
    @Inject
    private var eventBus: EventBus
) {

    val logger: Logger = LoggerFactory.getLogger(this::class.java.toString())

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    fun hello(): String {
        //Blocking Response (I/O Thread)
        emitter.send("Hello world")

        //Async - Does not block response - internal messaging
        eventBus.publish("greetings", "Hello world")

        //Blocking Response (I/O Thread)
        val uni = thingListener("message")
            .subscribe().with { item -> logger.debug(">> {}", item) }

        logger.debug("Sending response")
        return "Hello world"
    }

    fun thingListener(thing: String): Uni<Void> = Uni.createFrom()
        .item("Hello world")
        .onItem().transform { item -> null }

    @Incoming("exception")
    fun channelListener(thing: String): Unit {
        logger.debug("Received $thing")
    }

    @ConsumeEvent("greetings")
    fun eventConsumer(event: String) {
        logger.debug("Received event {}", event)
        TimeUnit.SECONDS.sleep(2) //Mimic API or external call (BigQuery write or FileSystem)
        logger.debug("Actioned event event {}", event)
    }
}
