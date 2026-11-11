package com.example.rocketmq4.service;

import com.example.rocketmq4.common.MqConst;
import com.example.rocketmq4.dto.OrderEvent;
import com.example.rocketmq4.dto.PlaceOrderCommand;
import com.example.rocketmq4.order.OrderRecord;
import com.example.rocketmq4.order.OrderRepository;
import com.example.rocketmq4.order.OrderStatus;
import com.example.rocketmq4.producer.OrderEventProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 订单业务入口。
 *
 * <p>这里刻意不把「更新数据库」和「发消息」放进同一个方法级事务：
 * 数据库事务提交与消息发送无法靠本地事务保证原子性，
 * 真正可靠的做法是事务消息（下单）或本地消息表（其它事件）。
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderEventProducer producer;
    private final AtomicLong flowSequence = new AtomicLong();

    public OrderService(OrderRepository orderRepository, OrderEventProducer producer) {
        this.orderRepository = orderRepository;
        this.producer = producer;
    }

    /**
     * 下单：通过事务消息保证「订单落库」与「创建事件投递」最终一致。
     *
     * <p>执行顺序：发半消息（对消费者不可见）-> 执行本地事务创建订单
     * -> 提交半消息（消费者可见）。本地事务失败则回滚半消息，消息不会投递。
     */
    public OrderEvent placeOrder(PlaceOrderCommand command) {
        // 先做一次快速前置校验，把重复提交挡在发消息之前，省掉一次事务消息开销
        if (orderRepository.exists(command.getOrderId())) {
            throw new IllegalStateException("订单已存在，请勿重复提交：" + command.getOrderId());
        }

        long now = System.currentTimeMillis();
        OrderEvent event = OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(command.getOrderId())
                .eventType("CREATED")
                .status(OrderStatus.CREATED)
                .userId(command.getUserId())
                .amount(command.getAmount())
                .eventTime(now)
                .schemaVersion(1)
                .build();

        TransactionSendResult result = producer.sendInTransaction(MqConst.ORDER_TX_TOPIC, event, command);
        // 只有明确提交才继续；UNKNOWN 表示 Broker 稍后会回查，此时补发延迟消息没有意义
        if (result.getLocalTransactionState() != LocalTransactionState.COMMIT_MESSAGE) {
            throw new IllegalStateException("订单创建未生效（本地事务已回滚），orderId=" + command.getOrderId());
        }

        // 本地事务已提交，补发延迟关单消息。即使这条消息发送失败，也只是"关不了单"，
        // 不会影响订单主流程，由兜底定时任务补偿即可。
        OrderEvent timeoutEvent = OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(command.getOrderId())
                .eventType("TIMEOUT_CHECK")
                .status(OrderStatus.CREATED)
                .userId(command.getUserId())
                .amount(command.getAmount())
                .eventTime(System.currentTimeMillis())
                .schemaVersion(1)
                .build();
        producer.sendDelay(MqConst.ORDER_TIMEOUT_TOPIC, timeoutEvent, producer.getDelayLevel());

        return event;
    }

    /**
     * 支付：条件更新订单状态，成功后发送两条不同 Tag 的消息。
     *
     * <p>updateStatus 返回 0 说明订单已支付或已关闭，直接拒绝，避免重复支付。
     */
    public void payOrder(String orderId) {
        long now = System.currentTimeMillis();
        int rows = orderRepository.updateStatus(orderId, OrderStatus.CREATED, OrderStatus.PAID, now);
        if (rows == 0) {
            throw new IllegalStateException("订单不存在或当前状态不允许支付：" + orderId);
        }

        OrderRecord order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalStateException("订单不存在：" + orderId));

        // Tag=paid：普通消息，通知营销、积分等下游
        producer.sendSync(MqConst.destination(MqConst.ORDER_TOPIC, MqConst.TAG_PAID),
                buildEvent(order, "PAID"));

        // Tag=status：顺序消息，同一个 orderId 的状态变更必须有序
        producer.sendOrderly(MqConst.destination(MqConst.ORDER_TOPIC, MqConst.TAG_STATUS),
                buildEvent(order, "STATUS_CHANGED"), orderId);
    }

    /**
     * 发送一串状态流转事件，用于验证顺序消费。
     */
    public int sendStatusFlow(String orderId, int count) {
        OrderRecord order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("订单不存在：" + orderId));

        for (int i = 1; i <= count; i++) {
            OrderEvent event = buildEvent(order, "STATUS_CHANGED");
            event.setRemark("FLOW-" + flowSequence.addAndGet(1) + "-SEQ-" + i);
            producer.sendOrderly(MqConst.destination(MqConst.ORDER_TOPIC, MqConst.TAG_STATUS), event, orderId);
        }
        return count;
    }

    /**
     * 支付/关单前必须带上前置状态做条件更新，这是并发下的第一道防线。
     */
    public boolean closeOrderIfUnpaid(String orderId) {
        return orderRepository.updateStatus(orderId, OrderStatus.CREATED, OrderStatus.CLOSED,
                System.currentTimeMillis()) > 0;
    }

    public Optional<OrderRecord> findOrder(String orderId) {
        return orderRepository.findById(orderId);
    }

    public List<OrderRecord> listOrders() {
        return orderRepository.findAll();
    }

    private OrderEvent buildEvent(OrderRecord order, String eventType) {
        return OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(order.getOrderId())
                .eventType(eventType)
                .status(order.getStatus())
                .userId(order.getUserId())
                .amount(order.getAmount())
                .eventTime(System.currentTimeMillis())
                .schemaVersion(1)
                .build();
    }
}
