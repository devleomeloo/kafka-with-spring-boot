package com.kafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.domain.Book;
import com.kafka.domain.LibraryEvent;
import com.kafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventsControllerUnitTest {

    String libraryEventsPath = "/libraries/events/v1";

    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void POSTLibraryEvent() throws Exception {
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

        String json = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class)))
                .thenReturn(null);
        //act

        mockMvc.perform(post(libraryEventsPath)
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }

    @Test
    void POSTLibraryEvent_4xx() throws Exception {
        //arrange
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName("Kafka using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String errorMessage = "book.bookAuthor - Book Author can't be blank !, book.bookId - Book Id can't be null !";

        String json = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class)))
                .thenReturn(null);

        //act

        mockMvc.perform(post(libraryEventsPath)
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(errorMessage));
    }
}