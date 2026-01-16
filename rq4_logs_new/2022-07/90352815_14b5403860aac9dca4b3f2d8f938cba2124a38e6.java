package org.rutebanken.irkalla.routes.kafka;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.http.common.HttpMethods;
import org.rutebanken.irkalla.Constants;
import org.rutebanken.irkalla.avro.StopPlaceChangelogEvent;
import org.rutebanken.irkalla.routes.BaseRouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaStopPlaceChangeRouteBuilder extends BaseRouteBuilder {

    @Autowired
    private StopPlaceChangeEventFactory stopPlaceChangeEventFactory;

    @Override
    public void configure() throws Exception {
        super.configure();

        from("direct:processChangedStopPlacesAsNetexKafka")
                .log(LoggingLevel.INFO,"Process changed StopPlace ${header." + Constants.HEADER_ENTITY_ID + "} to Kafka")
                .log(LoggingLevel.INFO,"Exchange body is ${body}")
                .setHeader(Exchange.HTTP_METHOD, constant(HttpMethods.GET))
                //.setBody(constant(null))
                .setHeader("stopPlaceId", simple("${header." + Constants.HEADER_ENTITY_ID + "}"))
                .to("direct:notifyConsumers")
                .routeId("process-changed-stop-place-kafka");


        from("direct:notifyConsumers")
                .log(LoggingLevel.INFO, "Notifying Kafka topic ${properties:irkalla.kafka.topic.event}")
                .bean(stopPlaceChangeEventFactory,"createStopPlaceChangelogEvent")
                .to("kafka:{{irkalla.kafka.topic.event}}?clientId=irkalla-event&valueSerializer=io.confluent.kafka.serializers.KafkaAvroSerializer").id("to-kafka-topic-event")
                .log(LoggingLevel.INFO,   "Notified changelog: ${body}")
                .routeId("notify-consumers");

        from("kafka:{{irkalla.kafka.topic.event}}?clientId=irkalla-event-reader&valueDeserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer&specificAvroReader=true&seekTo=beginning&autoOffsetReset=earliest&offsetRepository=#irkallaEventReaderOffsetRepo")
                .log(LoggingLevel.INFO,  "Received notification event from ${properties:irkalla.kafka.topic.event}")
                .to("direct:processStopPlaceChangelog")
                .routeId("from-kafka-topic-event");

        from("direct:processStopPlaceChangelog")
                .process(exchange -> {
                    final StopPlaceChangelogEvent stopPlaceChangelogEvent = exchange.getIn().getBody(StopPlaceChangelogEvent.class);
                    log.info("Event from kafka: stopPlaceId: {}, version: {}, eventType: {} ",stopPlaceChangelogEvent.getStopPlaceId(),stopPlaceChangelogEvent.getStopPlaceVersion(),stopPlaceChangelogEvent.getEventType());
                })
                .routeId("process-stop-place-changelog");


    }

}