package com.example.kafkaproducer.service;

import com.example.kafkaproducer.vo.EffectedAd;
import com.example.kafkaproducer.vo.PurchaseLog;
import com.example.kafkaproducer.vo.PurchaseLogOneProduct;
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
    // EffectedAd --> Json 형태로 Topic : AdEvaluationComplete

    private final ProduceService produceService;


    @Autowired
    public void buildPipeline(StreamsBuilder sb) {
        JsonSerializer<EffectedAd> effectOrNotJsonSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLog> purchaseLogJsonSerializer = new JsonSerializer<>();
        JsonSerializer<WatchingAdLog> watchingAdLogJsonSerializer = new JsonSerializer<>();
        JsonSerializer<PurchaseLogOneProduct> purchaseLogOneProductJsonSerializer = new JsonSerializer<>();

        JsonDeserializer<EffectedAd> effectOrNotJsonDeserializer = new JsonDeserializer<>();
        JsonDeserializer<PurchaseLog> purchaseLogJsonDeSerializer = new JsonDeserializer<>();
        JsonDeserializer<WatchingAdLog> watchingAdLogJsonDeSerializer = new JsonDeserializer<>();
        JsonDeserializer<PurchaseLogOneProduct> purchaseLogOneProductJsonDeserializer = new JsonDeserializer<>();

        Serde<EffectedAd> effectOrNotSerde = Serdes.serdeFrom(effectOrNotJsonSerializer, effectOrNotJsonDeserializer);
        Serde<PurchaseLog> purchaseLogSerde = Serdes.serdeFrom(purchaseLogJsonSerializer, purchaseLogJsonDeSerializer);
        Serde<WatchingAdLog> watchingAdLogSerde = Serdes.serdeFrom(watchingAdLogJsonSerializer, watchingAdLogJsonDeSerializer);
        Serde<PurchaseLogOneProduct> purchaseLogOneProductSerde = Serdes.serdeFrom(purchaseLogOneProductJsonSerializer, purchaseLogOneProductJsonDeserializer);


        KTable<String, WatchingAdLog> adTable = sb.stream("AdLog", Consumed.with(Serdes.String(), watchingAdLogSerde))
                .selectKey(((key, value) -> value.getUserId() + "_" + value.getProductId()))
                .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[]>>as("adStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(watchingAdLogSerde)
                );

        sb.stream("PurchaseLog", Consumed.with(Serdes.String(), purchaseLogSerde)) // product가 List형태로 있기때문에 한 productId당 새로 Publish
                .filter((key, value) -> value.getPrice() < 1000000)
                .foreach(((key, value) -> {
                    for (String productId : value.getProductId()) {
                        PurchaseLogOneProduct purchaseLogOneProduct = PurchaseLogOneProduct.builder()
                                .productId(productId)
                                .userId(value.getUserId())
                                .price(value.getPrice())
                                .purchaseDt(value.getPurchaseDt())
                                .orderId(value.getOrderId())
                                .build();
                        produceService.sendJoinedMsg("oneProduct", purchaseLogOneProduct);
                    }
                }));

        ValueJoiner<WatchingAdLog, PurchaseLogOneProduct, EffectedAd> tableJoiner = (leftValue, rightValue) -> EffectedAd.builder()
                .userId(rightValue.getUserId())
                .adId(leftValue.getAdId())
                .build();

        KTable<String, PurchaseLogOneProduct> purchaseLogOneProductKTable = sb.stream("oneProduct", Consumed.with(Serdes.String(), purchaseLogOneProductSerde))
                .selectKey(((key, value) -> value.getUserId() + "_" + value.getProductId()))
                .toTable(Materialized.<String, PurchaseLogOneProduct, KeyValueStore<Bytes, byte[]>>as("adStore")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(purchaseLogOneProductSerde));

        adTable.join(purchaseLogOneProductKTable, tableJoiner).toStream().to("AdEvaluationComplete", Produced.with(Serdes.String(), effectOrNotSerde));
    }


}
