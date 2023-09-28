package com.example.kafkaproducer.config;

import com.example.kafkaproducer.util.CustomSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> myConfig = new HashMap<>();
        myConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-test");
        myConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3); //몇개의 쓰레드로 돌릴것인가
        myConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19094, localhost:29094, localhost:39094");
        myConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        myConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //Producer, Consumer와 달리 Stream은 받고 보내고를 한번에 하기때문에
        // 원래 SERIALIZER, DESERIALIZER를 따로따로 설정했던것과는 달리 Stream에서는 Serdes라는것으로 한번에 설정
        return new KafkaStreamsConfiguration(myConfig);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<String, Object>(producerFactory());
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> myConfig = new HashMap<>();
        myConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19094, localhost:29094, localhost:39094");
        myConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        myConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class);

        return new DefaultKafkaProducerFactory<>(myConfig);

    }
//
//    @Bean
//    public ConsumerFactory<String, Object> consumerFactory() {
//        Map<String, Object> myConfig = new HashMap<>();
//        myConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19094, localhost:29094, localhost:39094");
//        myConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        myConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//
//        return new DefaultKafkaConsumerFactory<>(myConfig);
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, Object> myFactory = new ConcurrentKafkaListenerContainerFactory<>();
//        myFactory.setConsumerFactory(consumerFactory());
//        return myFactory;
//    }
}
