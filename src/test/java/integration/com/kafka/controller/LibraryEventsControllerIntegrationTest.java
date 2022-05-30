package com.kafka.controller;

import com.kafka.domain.Book;
import com.kafka.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(
        properties = {
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.admin.properties.bootstrap-servers=${spring.embedded.kafka.brokers}"
        })
public class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private static Consumer<Integer,String> consumer;

    @BeforeEach
    void setUp() {
        //Configurando o Kafka Consumer para validar a msg de retorno
        Map<String, Object> configs =
                new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));

        consumer =
                new DefaultKafkaConsumerFactory<>(
                        configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();

        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterAll
    static void afterAll() {
        consumer.close();
    }

    @Test
    //Metodo para desligar a thread do getSingleRecord depois de 5 segundos
    @Timeout(5)
    void postLibraryEvent() throws InterruptedException {
        //arrange
        Book book = Book.builder()
                .bookId(1)
                .bookAuthor("LeoMelo")
                .bookName("Kafka using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

        String expectedValue = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":1,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"LeoMelo\"}}";

        //act
        ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange(
                "/libraries/events/v1", HttpMethod.POST, request, LibraryEvent.class);

        // Metodo responsavel por consumir os valores do topico quando forem produzidos
        ConsumerRecord<Integer, String> consumerRecord =
                KafkaTestUtils.getSingleRecord(consumer, "library-events");
        //Metodo para desligar a thread do getSingleRecord depois de 3 segundos
        //Thread.sleep(3000);

        //assert

        assertEquals(responseEntity.getStatusCode(), HttpStatus.CREATED);
        assertEquals(expectedValue, consumerRecord.value());
    }
}
