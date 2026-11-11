package com.example.rocketmq4.service;

import com.example.rocketmq4.dto.PlaceOrderCommand;
import com.example.rocketmq4.order.OrderRepository;
import com.example.rocketmq4.order.OrderStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 事务消息的本地事务执行体。
 *
 * <p>单独成类而不是放在 OrderService 里，是为了让 {@code @Transactional} 走 Spring 代理：
 * 同类内部方法自调用不会经过代理，事务注解会静默失效。
 */
@Service
public class OrderLocalTransactionService {

    private final OrderRepository orderRepository;

    public OrderLocalTransactionService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    /**
     * 创建订单，与业务库写入在同一个数据库事务中完成。
     * 抛出异常即表示本地事务失败，调用方应回滚半消息。
     */
    @Transactional(rollbackFor = Exception.class)
    public void createOrder(PlaceOrderCommand command) {
        orderRepository.insert(command, OrderStatus.CREATED, System.currentTimeMillis());
    }

    public boolean exists(String orderId) {
        return orderRepository.exists(orderId);
    }
}
