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
 * 事务消息消费者。
 *
 * <p>它收到的消息一定是"本地事务已提交"的那部分：半消息在本地事务提交前对消费者不可见，
 * 回滚的半消息永远不会投递到这里。
 */
@Component
@RocketMQMessageListener(
        topic = MqConst.ORDER_TX_TOPIC,
        consumerGroup = MqConst.GROUP_TX
)
public class OrderTxConsumer implements RocketMQListener<OrderEvent> {

    private static final Logger log = LoggerFactory.getLogger(OrderTxConsumer.class);

    private final OrderConsumeService consumeService;

    public OrderTxConsumer(OrderConsumeService consumeService) {
        this.consumeService = consumeService;
    }

    @Override
    public void onMessage(OrderEvent event) {
        try {
            consumeService.handleTxCreated(event);
        } catch (Exception e) {
            log.error("[消费失败] orderId={}, eventId={}", event.getOrderId(), event.getEventId(), e);
            throw new RuntimeException(e);
        }
    }
}
