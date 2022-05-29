package com.kafka.controller;

import com.kafka.domain.Book;
import com.kafka.domain.LibraryEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Test
    void postLibraryEvent() {
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

        //act
        ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange(
                "/libraries/events/v1", HttpMethod.POST, request, LibraryEvent.class);

        //assert
        assertEquals(responseEntity.getStatusCode(), HttpStatus.CREATED);

    }
}
