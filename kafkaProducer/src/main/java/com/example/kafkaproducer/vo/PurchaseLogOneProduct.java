package com.example.kafkaproducer.vo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PurchaseLogOneProduct {

    private String orderId;

    private String userId; // ui d-0001

    private String productId; // {pg-0001, pg-0002}

    private String purchaseDt; //202302010700

    private String price; // 24000

}
