package com.example.kafkaproducer.vo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
public class PurchaseLog {

    //{ "orderId": "od-0005", "userId": "uid-0005",  "productInfo": [{"productId": "pg-0023", "price":"12000"}, {"productId":"pg-0022", "price":"13500"}],  "purchaseDt": "20230201070000",  "price": 24000}
    private String orderId;

    private String userId; // ui d-0001

    private List<Map<String, String>> productInfo = new ArrayList<>();

    private String purchaseDt; //202302010700

    private Long price; // 24000


}
