package com.example.rocketmq4.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 订单领域事件，是所有生产/消费方共用的消息契约。
 *
 * <p>时间字段刻意使用 {@code long}（epoch 毫秒）而不是 {@code LocalDateTime}：
 * rocketmq-spring 的 MappingJackson2MessageConverter 内部自建 ObjectMapper，
 * 不会加载 Spring 容器里的 JavaTimeModule，用 LocalDateTime 会在序列化/反序列化时直接报错。
 *
 * <p>字段只增不改、新增字段给默认值，保证消息契约向后兼容。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 事件唯一标识，消费幂等键 */
    private String eventId;

    /** 订单号，业务聚合根标识 */
    private String orderId;

    /** 事件类型：CREATED / PAID / STATUS_CHANGED / CLOSED */
    private String eventType;

    /** 事件发生时的订单状态 */
    private String status;

    private String userId;

    private BigDecimal amount;

    /** 事件发生时间，epoch 毫秒 */
    private long eventTime;

    /** 消息结构版本，便于消费端做兼容分支 */
    private int schemaVersion;

    /** 备注，可用于传递调试信息 */
    private String remark;
}
