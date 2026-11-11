package com.example.rocketmq4.producer;

import com.example.rocketmq4.common.MqConst;
import com.example.rocketmq4.dto.OrderEvent;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

/**
 * 消息发送统一出口。
 *
 * <p>把 5 种发送方式收敛到一处，业务代码只调语义方法，不直接接触 RocketMQTemplate，
 * 后续要统一加追踪、埋点、降级时也只需改这一个类。
 */
@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    /** 同步发送超时时间，与 rocketmq.producer.send-message-timeout 保持一致的量级 */
    private static final long SEND_TIMEOUT = 3000L;

    private final RocketMQTemplate rocketMQTemplate;
    private final int delayLevel;

    public OrderEventProducer(RocketMQTemplate rocketMQTemplate,
                              @Value("${demo.mq.delay-level:3}") int delayLevel) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.delayLevel = delayLevel;
    }

    /**
     * 构造消息并写入 KEYS。KEYS 用于控制台按业务键查消息，不是幂等键。
     */
    private Message<OrderEvent> buildMessage(OrderEvent event) {
        return MessageBuilder.withPayload(event)
                .setHeader(RocketMQHeaders.KEYS, event.getOrderId())
                .build();
    }

    /**
     * 同步发送：等待 Broker 返回存储结果，适合需要确认结果的业务。
     */
    public SendResult sendSync(String destination, OrderEvent event) {
        SendResult result = rocketMQTemplate.syncSend(destination, buildMessage(event), SEND_TIMEOUT);
        log.info("[同步发送] destination={}, msgId={}, status={}",
                destination, result.getMsgId(), result.getSendStatus());
        return result;
    }

    /**
     * 异步发送：不阻塞业务线程，但必须处理失败回调。
     */
    public void sendAsync(String destination, OrderEvent event) {
        rocketMQTemplate.asyncSend(destination, buildMessage(event), new SendCallback() {
            @Override
            public void onSuccess(SendResult result) {
                log.info("[异步发送-成功] destination={}, msgId={}", destination, result.getMsgId());
            }

            @Override
            public void onException(Throwable e) {
                // 生产环境这里要落库并进入补偿任务，不能只打日志
                log.error("[异步发送-失败] destination={}, orderId={}", destination, event.getOrderId(), e);
            }
        }, SEND_TIMEOUT);
    }

    /**
     * 单向发送：不等待任何确认，只适合埋点、日志这类允许少量丢失的场景。
     */
    public void sendOneWay(String destination, OrderEvent event) {
        rocketMQTemplate.sendOneWay(destination, buildMessage(event));
        log.info("[单向发送] destination={}, orderId={}", destination, event.getOrderId());
    }

    /**
     * 顺序发送：以 hashKey 选择队列，同一个 orderId 必然进入同一队列。
     */
    public SendResult sendOrderly(String destination, OrderEvent event, String hashKey) {
        SendResult result = rocketMQTemplate.syncSendOrderly(destination, buildMessage(event), hashKey, SEND_TIMEOUT);
        log.info("[顺序发送] destination={}, hashKey={}, queueId={}",
                destination, hashKey, result.getMessageQueue() == null ? -1 : result.getMessageQueue().getQueueId());
        return result;
    }

    /**
     * 延迟发送：RocketMQ 4.x 只支持 Broker 预置的延迟等级，不支持任意精确时间。
     */
    public SendResult sendDelay(String destination, OrderEvent event, int level) {
        SendResult result = rocketMQTemplate.syncSend(destination, buildMessage(event), SEND_TIMEOUT, level);
        log.info("[延迟发送] destination={}, delayLevel={}, msgId={}", destination, level, result.getMsgId());
        return result;
    }

    /**
     * 事务消息：先发半消息，再执行本地事务，由 Broker 根据本地事务状态决定是否投递。
     */
    public TransactionSendResult sendInTransaction(String destination, OrderEvent event, Object arg) {
        TransactionSendResult result = rocketMQTemplate.sendMessageInTransaction(destination, buildMessage(event), arg);
        log.info("[事务消息] destination={}, localState={}, msgId={}",
                destination, result.getLocalTransactionState(), result.getMsgId());
        return result;
    }

    public int getDelayLevel() {
        return delayLevel;
    }
}
