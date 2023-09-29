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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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

        JsonDeserializer<EffectedAd> effectOrNotJsonDeserializer = new JsonDeserializer<>(EffectedAd.class);
        JsonDeserializer<PurchaseLog> purchaseLogJsonDeSerializer = new JsonDeserializer<>(PurchaseLog.class);
        JsonDeserializer<WatchingAdLog> watchingAdLogJsonDeSerializer = new JsonDeserializer<>(WatchingAdLog.class);
        JsonDeserializer<PurchaseLogOneProduct> purchaseLogOneProductJsonDeserializer = new JsonDeserializer<>(PurchaseLogOneProduct.class);

        Serde<EffectedAd> effectOrNotSerde = Serdes.serdeFrom(effectOrNotJsonSerializer, effectOrNotJsonDeserializer);
        Serde<PurchaseLog> purchaseLogSerde = Serdes.serdeFrom(purchaseLogJsonSerializer, purchaseLogJsonDeSerializer);
        Serde<WatchingAdLog> watchingAdLogSerde = Serdes.serdeFrom(watchingAdLogJsonSerializer, watchingAdLogJsonDeSerializer);
        Serde<PurchaseLogOneProduct> purchaseLogOneProductSerde = Serdes.serdeFrom(purchaseLogOneProductJsonSerializer, purchaseLogOneProductJsonDeserializer);


        KTable<String, WatchingAdLog> adTable = sb.stream("adLog", Consumed.with(Serdes.String(), watchingAdLogSerde))
                .selectKey(((key, value) -> value.getUserId() + "_" + value.getProductId()))
                .toTable(Materialized.<String, WatchingAdLog, KeyValueStore<Bytes, byte[]>>as("adStore1")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(watchingAdLogSerde)
                );

        sb.stream("purchaseLog", Consumed.with(Serdes.String(), purchaseLogSerde)) // product가 List형태로 있기때문에 한 productId당 새로 Publish
                .foreach((key, value) -> {
                    for (Map<String, String> product : value.getProductInfo()) {
                        if (Integer.parseInt(product.get("price")) < 1000000) {
                            PurchaseLogOneProduct purchaseLogOneProduct = PurchaseLogOneProduct.builder()
                                    .productId(product.get("productId"))
                                    .userId(value.getUserId())
                                    .price(product.get("price"))
                                    .purchaseDt(value.getPurchaseDt())
                                    .orderId(value.getOrderId())
                                    .build();
                            produceService.sendJoinedMsg("oneProduct", purchaseLogOneProduct);
                            //sendNewMsg();
                        }
                    }
                });

        ValueJoiner<WatchingAdLog, PurchaseLogOneProduct, EffectedAd> tableJoiner = (leftValue, rightValue) -> {
            log.info("joining...");

            return EffectedAd.builder()
                    .userId(rightValue.getUserId())
                    .adId(leftValue.getAdId())
                    .orderId(rightValue.getOrderId())
                    .productInfo(Map.of("productId", rightValue.getProductId(), "price", rightValue.getPrice()))
                    .build();
        };

        KTable<String, PurchaseLogOneProduct> purchaseLogOneProductKTable = sb.stream("oneProduct", Consumed.with(Serdes.String(), purchaseLogOneProductSerde))
                .selectKey(((key, value) -> value.getUserId() + "_" + value.getProductId()))
                .toTable(Materialized.<String, PurchaseLogOneProduct, KeyValueStore<Bytes, byte[]>>as("purchaseLogStore1")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(purchaseLogOneProductSerde));

        adTable.join(purchaseLogOneProductKTable, tableJoiner).toStream().to("AdEvaluationComplete", Produced.with(Serdes.String(), effectOrNotSerde));
    }

    public void sendNewMsg() {
        PurchaseLog tempPurchaseLog  = new PurchaseLog();
        WatchingAdLog tempWatchingAdLog = new WatchingAdLog();

        //랜덤한 ID를 생성하기 위해 아래의 함수를 사용합니다.
        // random Numbers for concatenation with attrs
        Random rd = new Random();
        int rdUidNumber = rd.nextInt(9999);
        int rdOrderNumber = rd.nextInt(9999);
        int rdProdIdNumber = rd.nextInt(9999);
        int rdPriceIdNumber = rd.nextInt(90000)+10000;
        int prodCnt = rd.nextInt(9)+1;
        int watchingTime = rd.nextInt(55)+5;

        // bind value for purchaseLog
        tempPurchaseLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempPurchaseLog.setPurchaseDt("20230101070000");
        tempPurchaseLog.setOrderId("od-" + String.format("%05d", rdOrderNumber));
        ArrayList<Map<String, String>> tempProdInfo = new ArrayList<>();
        Map<String, String> tempProd = new HashMap<>();
        for (int i=0; i<prodCnt; i++ ){
            tempProd.put("productId", "pg-" + String.format("%05d", rdProdIdNumber));
            tempProd.put("price", String.format("%05d", rdPriceIdNumber));
            tempProdInfo.add(tempProd);
        }
        tempPurchaseLog.setProductInfo(tempProdInfo);

        // bind value for watchingAdLog
        tempWatchingAdLog.setUserId("uid-" + String.format("%05d", rdUidNumber));
        tempWatchingAdLog.setProductId("pg-" + String.format("%05d", rdProdIdNumber));
        tempWatchingAdLog.setAdId("ad-" + String.format("%05d",rdUidNumber) );
        tempWatchingAdLog.setAdType("banner");
        tempWatchingAdLog.setWatchingTime(String.valueOf(watchingTime));
        tempWatchingAdLog.setWatchingDt("20230201070000");

        // produce msg
        produceService.sendMsgForPurchaseLog("purchaseLog", tempPurchaseLog);
        produceService.sendMsgForWatchingAdLog("adLog", tempWatchingAdLog);
    }

}
