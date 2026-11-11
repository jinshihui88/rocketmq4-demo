package com.example.rocketmq4.tx;

import com.example.rocketmq4.dto.PlaceOrderCommand;
import com.example.rocketmq4.service.OrderLocalTransactionService;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.messaging.Message;

/**
 * 事务消息监听器：负责执行本地事务，并响应 Broker 的事务回查。
 *
 * <p>rocketMQTemplateBeanName 默认即 "rocketMQTemplate"，
 * 它会与同名模板的 producer group 绑定，因此本地事务只会被这个组触发。
 */
@RocketMQTransactionListener
public class OrderTransactionListener implements RocketMQLocalTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(OrderTransactionListener.class);

    private final OrderLocalTransactionService localTransactionService;

    public OrderTransactionListener(OrderLocalTransactionService localTransactionService) {
        this.localTransactionService = localTransactionService;
    }

    /**
     * 执行本地事务。返回值决定半消息是否投递给消费者。
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object arg) {
        PlaceOrderCommand command = (PlaceOrderCommand) arg;
        try {
            localTransactionService.createOrder(command);
            log.info("[本地事务] 订单创建成功，提交半消息，orderId={}", command.getOrderId());
            return RocketMQLocalTransactionState.COMMIT;
        } catch (DuplicateKeyException e) {
            // 主键冲突说明这笔订单已经落库，属于并发重复提交：
            // 回滚半消息，避免下游再收到一条重复的创建事件
            log.warn("[本地事务] 订单已存在，回滚半消息，orderId={}", command.getOrderId());
            return RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            log.error("[本地事务] 订单创建失败，回滚半消息，orderId={}", command.getOrderId(), e);
            return RocketMQLocalTransactionState.ROLLBACK;
        }
    }

    /**
     * 事务回查。
     *
     * <p>Broker 长时间收不到二次确认时会回调这里，进程重启也不会丢：
     * 判定依据必须是数据库里的持久化状态，不能是内存变量。
     * 无法确定时返回 UNKNOWN，Broker 会继续回查，而不是猜一个结果。
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        Object keys = message.getHeaders().get(RocketMQHeaders.KEYS);
        String orderId = keys == null ? null : keys.toString();
        if (orderId == null || orderId.isEmpty()) {
            log.warn("[事务回查] 消息缺少 KEYS，无法判定本地事务状态，返回 UNKNOWN");
            return RocketMQLocalTransactionState.UNKNOWN;
        }

        boolean exists = localTransactionService.exists(orderId);
        log.info("[事务回查] orderId={}, 本地事务已提交={}", orderId, exists);
        return exists ? RocketMQLocalTransactionState.COMMIT : RocketMQLocalTransactionState.ROLLBACK;
    }
}
