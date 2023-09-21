package com.example.kafkaproducer.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProduceService {

    String myTopic = "fastCampus";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publish(String msg) {
        kafkaTemplate.send(myTopic, msg);
    }
}
 