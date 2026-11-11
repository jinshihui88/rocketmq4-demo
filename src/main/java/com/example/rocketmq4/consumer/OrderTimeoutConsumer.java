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
 * 延迟消息消费者：订单超时关单。
 *
 * <p>延迟消息只是"定时触发器"，消费时必须回查数据库真实状态，
 * 而不是相信"消息到了就一定该关单"——订单可能早就支付了。
 */
@Component
@RocketMQMessageListener(
        topic = MqConst.ORDER_TIMEOUT_TOPIC,
        consumerGroup = MqConst.GROUP_TIMEOUT
)
public class OrderTimeoutConsumer implements RocketMQListener<OrderEvent> {

    private static final Logger log = LoggerFactory.getLogger(OrderTimeoutConsumer.class);

    private final OrderConsumeService consumeService;

    public OrderTimeoutConsumer(OrderConsumeService consumeService) {
        this.consumeService = consumeService;
    }

    @Override
    public void onMessage(OrderEvent event) {
        try {
            consumeService.handleTimeoutCheck(event);
        } catch (Exception e) {
            log.error("[消费失败] orderId={}, eventId={}", event.getOrderId(), event.getEventId(), e);
            throw new RuntimeException(e);
        }
    }
}
