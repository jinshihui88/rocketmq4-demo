package com.example.rocketmq4.order;

/**
 * 订单状态。状态流转只允许 CREATED -> PAID / CLOSED，由条件更新保证。
 */
public final class OrderStatus {

    private OrderStatus() {
    }

    public static final String CREATED = "CREATED";
    public static final String PAID = "PAID";
    public static final String CLOSED = "CLOSED";
}
