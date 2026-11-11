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
 * 支付成功事件消费者。
 *
 * <p>与 {@link OrderCreatedConsumer} 订阅同一个 Topic，但消费组和 Tag 都不同，
 * 这就是 RocketMQ 中「一个 Topic 多个订阅方」的标准做法：
 * 不同业务方用不同消费组，各自维护自己的消费位点，互不影响。
 */
@Component
@RocketMQMessageListener(
        topic = MqConst.ORDER_TOPIC,
        consumerGroup = MqConst.GROUP_PAID,
        selectorExpression = MqConst.TAG_PAID
)
public class OrderPaidConsumer implements RocketMQListener<OrderEvent> {

    private static final Logger log = LoggerFactory.getLogger(OrderPaidConsumer.class);

    private final OrderConsumeService consumeService;

    public OrderPaidConsumer(OrderConsumeService consumeService) {
        this.consumeService = consumeService;
    }

    @Override
    public void onMessage(OrderEvent event) {
        try {
            consumeService.handlePaid(event);
        } catch (Exception e) {
            log.error("[消费失败] orderId={}, eventId={}", event.getOrderId(), event.getEventId(), e);
            throw new RuntimeException(e);
        }
    }
}
