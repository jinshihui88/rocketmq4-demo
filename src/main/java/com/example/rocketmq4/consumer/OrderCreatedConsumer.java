package com.example.rocketmq4.consumer;

import com.example.rocketmq4.common.MqConst;
import com.example.rocketmq4.dto.OrderEvent;
import com.example.rocketmq4.service.OrderConsumeService;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 订单创建事件消费者（普通消息 + Tag 过滤）。
 *
 * <p>selectorExpression = "created" 表示只订阅 order-topic 下 Tag 为 created 的消息，
 * 同一个 Topic 下的 paid / status 不会投递给这个消费组。
 */
@Component
@RocketMQMessageListener(
        topic = MqConst.ORDER_TOPIC,
        consumerGroup = MqConst.GROUP_CREATED,
        selectorExpression = MqConst.TAG_CREATED
)
public class OrderCreatedConsumer implements RocketMQListener<OrderEvent> {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedConsumer.class);

    private final OrderConsumeService consumeService;

    public OrderCreatedConsumer(OrderConsumeService consumeService) {
        this.consumeService = consumeService;
    }

    @Override
    public void onMessage(OrderEvent event) {
        try {
            consumeService.handleCreated(event);
        } catch (Exception e) {
            // 必须抛出，否则框架会认为消费成功并提交位点，消息就永久丢了
            log.error("[消费失败] orderId={}, eventId={}", event.getOrderId(), event.getEventId(), e);
            throw new RuntimeException(e);
        }
    }
}
