package com.example.rocketmq4.order;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 订单只读视图。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderRecord {

    private String orderId;
    private String userId;
    private BigDecimal amount;
    private String status;
    private long createdAt;
    private long updatedAt;
}
