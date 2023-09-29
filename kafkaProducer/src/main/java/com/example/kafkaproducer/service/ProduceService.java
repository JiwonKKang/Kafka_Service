package com.example.kafkaproducer.service;

import com.example.kafkaproducer.config.KafkaConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProduceService {

    String myTopic = "fastCampus";

    private final KafkaConfig myConfig;

    private KafkaTemplate<String, Object> kafkaTemplate;

    public void pub(String msg) {
        kafkaTemplate = myConfig.KafkaTemplateForGeneral();
        kafkaTemplate.send(myTopic, msg);
    }

    public void sendJoinedMsg (String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForGeneral();
        kafkaTemplate.send(topicNm, msg);
    }
    public void sendMsgForWatchingAdLog (String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForWatchingAdLog();
        kafkaTemplate.send(topicNm, msg);
    }

    public void sendMsgForPurchaseLog (String topicNm, Object msg) {
        kafkaTemplate = myConfig.KafkaTemplateForPurchaseLog();
        kafkaTemplate.send(topicNm, msg);
    }

}
 