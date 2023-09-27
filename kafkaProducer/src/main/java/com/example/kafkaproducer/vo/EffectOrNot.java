package com.example.kafkaproducer.vo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EffectOrNot {

    private String adId; //ad-101

    private String effectiveness; // y, n

    private String userId; //uid-0001


}
