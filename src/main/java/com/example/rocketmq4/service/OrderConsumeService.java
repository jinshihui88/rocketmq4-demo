package com.example.rocketmq4.service;

import com.example.rocketmq4.common.MqConst;
import com.example.rocketmq4.dto.OrderEvent;
import com.example.rocketmq4.order.ConsumeRecordRepository;
import com.example.rocketmq4.order.OrderNotifyRepository;
import com.example.rocketmq4.order.OrderRecord;
import com.example.rocketmq4.order.OrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消费处理逻辑。
 *
 * <p>所有方法都带 {@code @Transactional}，把「幂等登记」和「业务写入」放在同一个数据库事务里：
 * 业务失败 -> 事务回滚 -> 幂等记录一并回滚 -> 下次重试仍然可以正常处理。
 * 反过来，如果幂等记录单独提交，第一次失败的消费会把自己永久标记成"已消费"，
 * 这是线上最常见的踩坑点。
 *
 * <p>注意：{@code @Transactional} 必须由 Spring 代理调用才生效，
 * 所以 Consumer 只做转发，真正的处理放在这里，而不是写在 onMessage 里。
 */
@Service
public class OrderConsumeService {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumeService.class);

    private final ConsumeRecordRepository consumeRecordRepository;
    private final OrderNotifyRepository notifyRepository;
    private final OrderService orderService;

    /** 重试演示：同一个 eventId 的前 N 次消费强制失败，第 N+1 次成功 */
    private final boolean retryDemoEnabled;
    private final int retryDemoMaxTimes;
    private final ConcurrentMap<String, AtomicInteger> failCounter = new ConcurrentHashMap<>();

    public OrderConsumeService(ConsumeRecordRepository consumeRecordRepository,
                               OrderNotifyRepository notifyRepository,
                               OrderService orderService,
                               @Value("${demo.retry.enabled:false}") boolean retryDemoEnabled,
                               @Value("${demo.retry.max-times:2}") int retryDemoMaxTimes) {
        this.consumeRecordRepository = consumeRecordRepository;
        this.notifyRepository = notifyRepository;
        this.orderService = orderService;
        this.retryDemoEnabled = retryDemoEnabled;
        this.retryDemoMaxTimes = retryDemoMaxTimes;
    }

    /**
     * 处理订单创建事件（Tag=created 的普通消息）。
     */
    @Transactional(rollbackFor = Exception.class)
    public void handleCreated(OrderEvent event) {
        if (!firstConsume(MqConst.GROUP_CREATED, event)) {
            return;
        }
        failIfRetryDemo(event);
        notifyRepository.insert(event.getOrderId(), event.getEventId(), "CREATED", System.currentTimeMillis());
        log.info("[消费-created] 订单创建事件处理完成，orderId={}, userId={}, amount={}",
                event.getOrderId(), event.getUserId(), event.getAmount());
    }

    /**
     * 处理支付成功事件（Tag=paid 的普通消息）。
     */
    @Transactional(rollbackFor = Exception.class)
    public void handlePaid(OrderEvent event) {
        if (!firstConsume(MqConst.GROUP_PAID, event)) {
            return;
        }
        notifyRepository.insert(event.getOrderId(), event.getEventId(), "PAID", System.currentTimeMillis());
        log.info("[消费-paid] 支付成功事件处理完成，orderId={}", event.getOrderId());
    }

    /**
     * 顺序消费状态流转事件。
     *
     * <p>这里的日志顺序就是队列内的真实投递顺序，同一个 orderId 必然单调递增，
     * 不同 orderId 之间没有先后约束。
     */
    @Transactional(rollbackFor = Exception.class)
    public void handleStatusChanged(OrderEvent event) {
        if (!firstConsume(MqConst.GROUP_STATUS, event)) {
            return;
        }
        notifyRepository.insert(event.getOrderId(), event.getEventId(), "STATUS", System.currentTimeMillis());
        log.info("[消费-status] 顺序消息到达，orderId={}, remark={}", event.getOrderId(), event.getRemark());
    }

    /**
     * 处理延迟消息：到点后检查订单是否仍未支付，是则关单。
     *
     * <p>延迟消息只是"触发器"，真正的判定必须回查数据库当前状态，
     * 不能假设"消息到了就一定要关单"。
     */
    @Transactional(rollbackFor = Exception.class)
    public void handleTimeoutCheck(OrderEvent event) {
        if (!firstConsume(MqConst.GROUP_TIMEOUT, event)) {
            return;
        }
        // 延迟消息只是触发器，真实状态必须回查数据库
        OrderRecord order = orderService.findOrder(event.getOrderId()).orElse(null);
        if (order == null) {
            log.warn("[消费-timeout] 订单不存在，忽略超时消息，orderId={}", event.getOrderId());
            return;
        }
        if (OrderStatus.CREATED.equals(order.getStatus()) && orderService.closeOrderIfUnpaid(order.getOrderId())) {
            log.info("[消费-timeout] 订单超时未支付，已关闭，orderId={}", event.getOrderId());
        } else {
            log.info("[消费-timeout] 订单当前状态为 {}，无需关单，orderId={}",
                    order.getStatus(), event.getOrderId());
        }
    }

    /**
     * 处理事务消息投递过来的订单创建事件。
     */
    @Transactional(rollbackFor = Exception.class)
    public void handleTxCreated(OrderEvent event) {
        if (!firstConsume(MqConst.GROUP_TX, event)) {
            return;
        }
        notifyRepository.insert(event.getOrderId(), event.getEventId(), "TX_CREATED", System.currentTimeMillis());
        log.info("[消费-tx] 事务消息消费成功，orderId={}, eventId={}", event.getOrderId(), event.getEventId());
    }

    /**
     * 幂等登记。返回 false 表示这条消息此前已经处理过。
     */
    private boolean firstConsume(String consumerGroup, OrderEvent event) {
        boolean first = consumeRecordRepository.tryInsert(consumerGroup, event.getEventId(), System.currentTimeMillis());
        if (!first) {
            log.warn("[幂等拦截] 重复消息已忽略，group={}, eventId={}, orderId={}",
                    consumerGroup, event.getEventId(), event.getOrderId());
        }
        return first;
    }

    /**
     * 重试演示：抛出异常让框架感知失败并重新投递。
     *
     * <p>这个方法抛出的异常会带崩整个事务，包括上面刚写入的幂等记录，
     * 所以重试时仍然能重新走一遍完整流程。
     */
    private void failIfRetryDemo(OrderEvent event) {
        if (!retryDemoEnabled || !"RETRY_DEMO".equals(event.getEventType())) {
            return;
        }
        int times = failCounter.computeIfAbsent(event.getEventId(), key -> new AtomicInteger()).incrementAndGet();
        if (times <= retryDemoMaxTimes) {
            throw new IllegalStateException("模拟消费失败，第 " + times + " 次，eventId=" + event.getEventId());
        }
        log.info("[重试演示] 第 {} 次消费成功，eventId={}", times, event.getEventId());
    }
}
