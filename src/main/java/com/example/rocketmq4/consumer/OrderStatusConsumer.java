package com.example.rocketmq4.consumer;

import com.example.rocketmq4.common.MqConst;
import com.example.rocketmq4.dto.OrderEvent;
import com.example.rocketmq4.service.OrderConsumeService;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 状态流转事件消费者（顺序消费）。
 *
 * <p>{@link ConsumeMode#ORDERLY} 会按队列加锁顺序消费：同一个队列里前一条没消费完，
 * 后一条不会开始。配合生产端的 syncSendOrderly(hashKey=orderId)，
 * 就能保证同一订单的状态事件严格有序。
 *
 * <p>代价是：一条消息卡住会阻塞整个队列，所以顺序消费的业务逻辑必须短、必须可控，
 * 不要在里面等待外部接口超时。
 */
@Component
@RocketMQMessageListener(
        topic = MqConst.ORDER_TOPIC,
        consumerGroup = MqConst.GROUP_STATUS,
        selectorExpression = MqConst.TAG_STATUS,
        consumeMode = ConsumeMode.ORDERLY
)
public class OrderStatusConsumer implements RocketMQListener<OrderEvent> {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusConsumer.class);

    private final OrderConsumeService consumeService;

    public OrderStatusConsumer(OrderConsumeService consumeService) {
        this.consumeService = consumeService;
    }

    @Override
    public void onMessage(OrderEvent event) {
        try {
            consumeService.handleStatusChanged(event);
        } catch (Exception e) {
            log.error("[消费失败] orderId={}, eventId={}", event.getOrderId(), event.getEventId(), e);
            throw new RuntimeException(e);
        }
    }
}
