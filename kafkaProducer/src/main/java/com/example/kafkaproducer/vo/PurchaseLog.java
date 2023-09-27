package com.example.kafkaproducer.vo;

import lombok.Data;

import java.util.ArrayList;

@Data
public class PurchaseLog {

    private String orderId;

    private String userId; // ui d-0001

    private ArrayList<String> productId = new ArrayList<>(); // pg-0001

    private String purchaseDt; //202302010700

    private Long price; // 24000


}
