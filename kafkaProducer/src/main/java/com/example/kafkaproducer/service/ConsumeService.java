package com.example.kafkaproducer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumeService {

//    @KafkaListener(topics = "fastCampus", groupId = "spring")
//    public void consume(String message) {
//        log.info("Subscribed : {}", message);
//    }
}
