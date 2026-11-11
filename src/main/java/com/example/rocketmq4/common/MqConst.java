package com.example.rocketmq4.common;

/**
 * Topic / Tag / ConsumerGroup 常量。
 *
 * <p>注解属性只接受常量，因此这些名字必须以常量形式维护；
 * 一旦投产后修改，等同于更换一份订阅契约，需要同步迁移消费者组。
 */
public final class MqConst {

    private MqConst() {
    }

    /** 订单领域主 Topic，承载创建、支付、状态流转三类事件 */
    public static final String ORDER_TOPIC = "order-topic";

    /** 延迟消息 Topic，用于订单超时关单 */
    public static final String ORDER_TIMEOUT_TOPIC = "order-timeout-topic";

    /** 事务消息 Topic，用于订单创建与本地事务的一致性 */
    public static final String ORDER_TX_TOPIC = "order-tx-topic";

    /** 单向消息 Topic，仅用于可容忍丢失的埋点类场景 */
    public static final String METRICS_TOPIC = "metrics-topic";

    public static final String TAG_CREATED = "created";
    public static final String TAG_PAID = "paid";
    public static final String TAG_STATUS = "status";

    public static final String GROUP_CREATED = "order-created-consumer-group";
    public static final String GROUP_PAID = "order-paid-consumer-group";
    public static final String GROUP_STATUS = "order-status-consumer-group";
    public static final String GROUP_TIMEOUT = "order-timeout-consumer-group";
    public static final String GROUP_TX = "order-tx-consumer-group";

    /** 目的地写法：topic:tag */
    public static String destination(String topic, String tag) {
        return topic + ":" + tag;
    }
}
