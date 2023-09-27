package com.example.kafkaproducer.vo;

import lombok.Data;

@Data
public class WatchingAdLog {

    private String userId; // uid-0001

    private String productId; // pg-0001

    private String adId; // ad-101

    private String adType; // banner, clip, main

    private String watchingTime; //머문 시간

    private String watchingDt; // 20230201070000
}
