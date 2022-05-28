package com.kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.domain.LibraryEvent;
import com.kafka.producer.LibraryEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/libraries/events/v1")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {

        /* Chamada assíncrona para configurar o Kafka Template com mensagem de sucesso publica no topico
        libraryEventProducer.sendLibraryEvent(libraryEvent);*/

        //Chamada assíncrona para configurar o Kafka Template com mensagem de sucesso publica no topico usando o ProducerRecord
        libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);

        // Chamada síncrona para configurar o Kafka Template sem a necessidade de mensagem de validação
        //libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
