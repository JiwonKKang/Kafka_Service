package com.example.kafkaproducer.vo;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class EffectedAd {

    private String adId; //ad-101

    private String userId; //uid-0001

    private String orderId;

    private Map<String, String> productInfo;


}
