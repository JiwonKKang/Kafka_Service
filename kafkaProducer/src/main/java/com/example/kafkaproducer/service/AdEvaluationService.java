package com.example.kafkaproducer.service;

import com.example.kafkaproducer.vo.EffectOrNot;
import com.example.kafkaproducer.vo.PurchaseLog;
import com.example.kafkaproducer.vo.WatchingAdLog;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class AdEvaluationService {

    // 광고 데이터는 중복 Join될 필요 X -->  Table
    // 광고 이력이 먼저 PUB - 광고 -> 구매 가 일반적인 순서기때문
    // 구매 이력은 상품별로 들어오지않음 (복수개의 상품 존재) --> contain
    // 광고에 머문시간이 적어도 10초 이상되어야만 join 대상 -->
    // 특정 가격이상의 상품은 join 대상에서 제외 - 비싼상품은 이미 마음먹고 구매하러들어옴
    // 광고이력 : KTable(AdLog), 구매이력 : KStream(OrderLog)
    // filtering, 형 변환
    // EffectOrNot --> Json 형태로 Topic : AdEvaluationComplete





    @Autowired
    public void buildPipeline(StreamsBuilder sb) {
        JsonSerializer<EffectOrNot> effectOrNotJsonSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLog> purchaseLogJsonSerializer = new JsonSerializer<>();
        JsonSerializer<WatchingAdLog> watchingAdLogJsonSerializer = new JsonSerializer<>();

        JsonDeserializer<EffectOrNot> effectOrNotJsonDeserializer = new JsonDeserializer<>();
        JsonDeserializer<PurchaseLog> purchaseLogJsonDeSerializer = new JsonDeserializer<>();
        JsonDeserializer<WatchingAdLog> watchingAdLogJsonDeSerializer = new JsonDeserializer<>();

        Serde<EffectOrNot> effectOrNotSerde = Serdes.serdeFrom(effectOrNotJsonSerializer, effectOrNotJsonDeserializer);
        Serde<PurchaseLog> purchaseLogSerde = Serdes.serdeFrom(purchaseLogJsonSerializer, purchaseLogJsonDeSerializer);
        Serde<WatchingAdLog> watchingAdLogSerde = Serdes.serdeFrom(watchingAdLogJsonSerializer, watchingAdLogJsonDeSerializer);


        KTable<String, WatchingAdLog> adTable = sb.stream("AdLog", Consumed.with(Serdes.String(), watchingAdLogSerde))
                .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[]>>as("adStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(watchingAdLogSerde)
                );

        KStream<String, PurchaseLog> orderLogStream = sb.stream("OrderLog", Consumed.with(Serdes.String(), purchaseLogSerde))
                .filter((key, value) -> value.getPrice() < 1000000);

        ValueJoiner<WatchingAdLog, PurchaseLog, EffectOrNot> tableStreamJoiner = (lv, rv) -> {
            if (lv.getUserId().equals(rv.getUserId())) {
                EffectOrNot.builder()
                        .userId(lv.getUserId())
                        .adId(lv.getAdId())
                        .effectiveness("y");
            }
        }
    }

}
