package com.example.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(
    classes = {
        EventPublisher.class,
        EventKafkaConfig.class,
        KafkaEventContractTest.TestConfiguration.class
    },
    properties = {
        "event-kafka.enabled=true",
        "test.event.topic=example.events",
        "event-kafka.topic=${test.event.topic}",
        "event-kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
    }
)
@EmbeddedKafka(
    partitions = 1,
    topics = "${test.event.topic}",
    brokerProperties = "listeners=PLAINTEXT://localhost:0",
    bootstrapServersProperty = "spring.embedded.kafka.brokers"
)
@TestPropertySource(properties = "test.event.topic=example.events")

public class KafkaEventContractTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @EnableConfigurationProperties(EventKafkaProperties.class)
    static class TestConfiguration {}

    @Autowired
    private EventPublisher publisher;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void shouldDeliverEventMatchingConsumerContract() throws Exception {
        UUID entityId = UUID.randomUUID();
        String eventId = "11111111-1111-4111-8111-111111111111";
        String payload = """
            {
              "eventId": "%s",
              "eventKind": "domain-event",
              "occurredAt": "2025-01-01T10:00:00Z",
              "producer": "example-service",
              "correlationId": "test-correlation-1",
              "payload": {
                "sourceCode": "DEMO_SOURCE",
                "typeCode": "ENTITY_UPDATED",
                "data": {
                  "entityId": "42",
                  "test": true
                }
              }
            }
            """.formatted(eventId);

        var event = new OutboxEvent(
            UUID.randomUUID(),
            entityId,
            payload,
            0,
            OffsetDateTime.now()
        );

        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, "example-event-contract-test",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
            ),
            new StringDeserializer(),
            new StringDeserializer()
        ).createConsumer()) {
            embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
            publisher.publish(event);
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "example.events");

            assertThat(record.key()).isEqualTo(entityId.toString());

            // Simulates the consumer's envelope parsing and extraction.
            DecodedEvent parsed = decodeAndValidateEnvelope(record.value());

            Map<String, SourceDefinition> registeredSources = Map.of(
                "DEMO_SOURCE",
                new SourceDefinition("test-source-id", "DEMO_SOURCE", "DEFAULT_TYPE")
            );

            var processed = processEventForTest(record, parsed, registeredSources);

            assertThat(processed.topic()).isEqualTo("example.events");
            assertThat(processed.eventId()).isEqualTo(eventId);
            assertThat(processed.sourceId()).isEqualTo("test-source-id");
            assertThat(processed.sourceCode()).isEqualTo("DEMO_SOURCE");
            assertThat(processed.typeCode()).isEqualTo("ENTITY_UPDATED");
            assertThat(processed.payload()).isEqualTo(
                OBJECT_MAPPER.readTree(
                    """
                    {"entityId":"42","test":true}
                    """
                )
            );
            assertThat(processed.contextCode()).isEqualTo("DEMO_SOURCE");
            assertThat(processed.status()).isEqualTo("RECEIVED");

            assertThat(parsed.eventId()).isEqualTo(eventId);
            assertThat(parsed.sourceCode()).isEqualTo("DEMO_SOURCE");
            assertThat(parsed.typeCode()).isEqualTo("ENTITY_UPDATED");
            assertThat(parsed.correlationId()).isEqualTo("test-correlation-1");
            assertThat(parsed.data().path("entityId").asText()).isEqualTo("42");
            assertThat(parsed.data().path("test").asBoolean()).isTrue();
        }
    }

    private static DecodedEvent decodeAndValidateEnvelope(String json) throws Exception {
        JsonNode envelope = OBJECT_MAPPER.readTree(json);
        assertThat(envelope).isNotNull();
        assertThat(envelope.isObject()).isTrue();

        verifyFieldSet(
            envelope,
            Set.of("eventId", "eventKind", "occurredAt", "producer", "correlationId", "payload"),
            Set.of("eventId", "eventKind", "occurredAt", "producer", "payload")
        );

        String eventId = readRequiredText(envelope, "eventId");
        assertThat(UUID.fromString(eventId).toString()).isEqualToIgnoringCase(eventId);

        assertThat(readRequiredText(envelope, "eventKind")).isEqualTo("domain-event");
        OffsetDateTime.parse(readRequiredText(envelope, "occurredAt"));
        readNonBlankText(envelope, "producer");

        String correlationId = null;
        if (envelope.has("correlationId")) {
            correlationId = readNonBlankText(envelope, "correlationId");
        }

        JsonNode payload = envelope.path("payload");
        assertThat(payload.isObject()).isTrue();
        verifyFieldSet(
            payload,
            Set.of("sourceCode", "typeCode", "data"),
            Set.of("sourceCode", "data")
        );

        String sourceCode = readNonBlankText(payload, "sourceCode");
        String typeCode = payload.has("typeCode")
            ? readNonBlankText(payload, "typeCode")
            : null;

        return new DecodedEvent(
            eventId,
            sourceCode,
            typeCode,
            correlationId,
            payload.get("data")
        );
    }

    private static String readRequiredText(JsonNode object, String field) {
        JsonNode value = object.get(field);
        assertThat(value).as("field '%s' must be a string", field).isNotNull();
        assertThat(value.isTextual()).as("field '%s' must be a string", field).isTrue();
        return value.textValue();
    }

    private static String readNonBlankText(JsonNode object, String field) {
        String value = readRequiredText(object, field).trim();
        assertThat(value).as("field '%s' must not be blank", field).isNotEmpty();
        return value;
    }

    private static void verifyFieldSet(
        JsonNode object,
        Set<String> allowed,
        Set<String> required
    ) {
        Set<String> actual = new HashSet<>();
        object.fieldNames().forEachRemaining(actual::add);

        assertThat(actual).containsAll(required);
        assertThat(allowed).containsAll(actual);
    }

    private record DecodedEvent(
        String eventId,
        String sourceCode,
        String typeCode,
        String correlationId,
        JsonNode data
    ) {}

    private static ProcessedEvent processEventForTest(
        ConsumerRecord<String, String> record,
        DecodedEvent event,
        Map<String, SourceDefinition> registeredSources
    ) {
        SourceDefinition source = registeredSources.get(event.sourceCode());
        assertThat(source)
            .as("active event source '%s' must exist", event.sourceCode())
            .isNotNull();

        JsonNode context = event.data().path("contextCode");
        String contextCode = context.isTextual() && !context.asText().isBlank()
            ? context.asText().trim()
            : source.code();

        String typeCode = event.typeCode() != null
            ? event.typeCode()
            : source.typeCode();

        return new ProcessedEvent(
            record.topic(),
            record.partition(),
            Long.toString(record.offset()),
            event.eventId(),
            event.data(),
            source.id(),
            source.code(),
            typeCode,
            contextCode,
            "RECEIVED"
        );
    }

    private record SourceDefinition(
        String id,
        String code,
        String typeCode
    ) {}

    private record ProcessedEvent(
        String topic,
        int partition,
        String offset,
        String eventId,
        JsonNode payload,
        String sourceId,
        String sourceCode,
        String typeCode,
        String contextCode,
        String status
    ) {}

}
