package com.example.rocketmq4.controller;

import com.example.rocketmq4.common.ApiResult;
import com.example.rocketmq4.dto.OrderEvent;
import com.example.rocketmq4.dto.PlaceOrderCommand;
import com.example.rocketmq4.order.OrderRecord;
import com.example.rocketmq4.service.OrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.List;

/**
 * 订单相关接口，用于演示事务消息、顺序消息与延迟消息的真实链路。
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * 下单：走事务消息，返回时订单已落库、创建事件已提交。
     */
    @PostMapping
    public ApiResult<OrderEvent> place(@Valid @RequestBody PlaceOrderCommand command) {
        return ApiResult.ok(orderService.placeOrder(command));
    }

    /**
     * 支付：条件更新订单状态，并发两条不同 Tag 的消息。
     */
    @PostMapping("/{orderId}/pay")
    public ApiResult<Void> pay(@PathVariable String orderId) {
        orderService.payOrder(orderId);
        return ApiResult.ok();
    }

    /**
     * 连续发送 N 条状态流转事件，用于观察顺序消费效果。
     */
    @PostMapping("/{orderId}/status-flow")
    public ApiResult<Integer> statusFlow(@PathVariable String orderId,
                                         @RequestParam(defaultValue = "5") int count) {
        return ApiResult.ok(orderService.sendStatusFlow(orderId, count));
    }

    @GetMapping("/{orderId}")
    public ApiResult<OrderRecord> detail(@PathVariable String orderId) {
        return orderService.findOrder(orderId)
                .map(ApiResult::ok)
                .orElseGet(() -> ApiResult.fail(404, "订单不存在：" + orderId));
    }

    @GetMapping
    public ApiResult<List<OrderRecord>> list() {
        return ApiResult.ok(orderService.listOrders());
    }
}
