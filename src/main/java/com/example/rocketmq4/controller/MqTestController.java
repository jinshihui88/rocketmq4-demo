package com.example.rocketmq4.controller;

import com.example.rocketmq4.common.ApiResult;
import com.example.rocketmq4.common.MqConst;
import com.example.rocketmq4.dto.OrderEvent;
import com.example.rocketmq4.order.ConsumeRecordRepository;
import com.example.rocketmq4.order.OrderNotifyRepository;
import com.example.rocketmq4.producer.OrderEventProducer;
import com.example.rocketmq4.service.OrderService;
import org.apache.rocketmq.client.producer.SendResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 消息能力验证接口。
 *
 * <p>这组接口只用于本地联调和写教程时验证各种发送语义，
 * 生产环境不应该把这种"万能发送口"暴露在公网。
 */
@RestController
@RequestMapping("/api/mq")
public class MqTestController {

    private final OrderEventProducer producer;
    private final OrderService orderService;
    private final OrderNotifyRepository notifyRepository;
    private final ConsumeRecordRepository consumeRecordRepository;
    private final boolean retryDemoEnabled;

    public MqTestController(OrderEventProducer producer,
                            OrderService orderService,
                            OrderNotifyRepository notifyRepository,
                            ConsumeRecordRepository consumeRecordRepository,
                            @Value("${demo.retry.enabled:false}") boolean retryDemoEnabled) {
        this.producer = producer;
        this.orderService = orderService;
        this.notifyRepository = notifyRepository;
        this.consumeRecordRepository = consumeRecordRepository;
        this.retryDemoEnabled = retryDemoEnabled;
    }

    /**
     * 同步发送：返回 SendResult，可以直接看到 msgId 和落到的队列。
     */
    @PostMapping("/send-sync")
    public ApiResult<Map<String, Object>> sendSync(@RequestParam(required = false) String orderId) {
        OrderEvent event = demoEvent(orderId);
        SendResult result = producer.sendSync(
                MqConst.destination(MqConst.ORDER_TOPIC, MqConst.TAG_CREATED), event);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", event.getOrderId());
        data.put("eventId", event.getEventId());
        data.put("msgId", result.getMsgId());
        data.put("sendStatus", result.getSendStatus());
        data.put("queueId", result.getMessageQueue() == null ? -1 : result.getMessageQueue().getQueueId());
        return ApiResult.ok(data);
    }

    /**
     * 异步发送：接口立即返回，发送结果看应用日志。
     */
    @PostMapping("/send-async")
    public ApiResult<String> sendAsync(@RequestParam(required = false) String orderId) {
        OrderEvent event = demoEvent(orderId);
        producer.sendAsync(MqConst.destination(MqConst.ORDER_TOPIC, MqConst.TAG_CREATED), event);
        return ApiResult.ok("已提交异步发送，eventId=" + event.getEventId());
    }

    /**
     * 单向发送：不等待任何确认，只适合埋点类场景。
     */
    @PostMapping("/send-oneway")
    public ApiResult<String> sendOneWay(@RequestParam(required = false) String orderId) {
        OrderEvent event = demoEvent(orderId);
        producer.sendOneWay(MqConst.destination(MqConst.METRICS_TOPIC, MqConst.TAG_CREATED), event);
        return ApiResult.ok("已发出单向消息，eventId=" + event.getEventId());
    }

    /**
     * 延迟发送：delayLevel 依赖 Broker 的 messageDelayLevel 配置，默认 3 约等于 10 秒。
     */
    @PostMapping("/send-delay")
    public ApiResult<Map<String, Object>> sendDelay(@RequestParam(required = false) String orderId,
                                                    @RequestParam(required = false) Integer delayLevel) {
        OrderEvent event = demoEvent(orderId);
        int level = delayLevel == null ? producer.getDelayLevel() : delayLevel;
        SendResult result = producer.sendDelay(MqConst.ORDER_TIMEOUT_TOPIC, event, level);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", event.getOrderId());
        data.put("delayLevel", level);
        data.put("msgId", result.getMsgId());
        return ApiResult.ok(data);
    }

    /**
     * 重复发送：把同一个 eventId 连发 N 次，验证消费者幂等是否生效。
     * 预期结果：日志里出现 N-1 条"幂等拦截"，业务侧只生效一次。
     */
    @PostMapping("/send-duplicate")
    public ApiResult<Map<String, Object>> sendDuplicate(@RequestParam(defaultValue = "3") int times,
                                                        @RequestParam(required = false) String orderId) {
        if (times < 1 || times > 50) {
            throw new IllegalArgumentException("times 取值范围 1-50");
        }
        OrderEvent event = demoEvent(orderId);
        for (int i = 0; i < times; i++) {
            producer.sendSync(MqConst.destination(MqConst.ORDER_TOPIC, MqConst.TAG_CREATED), event);
        }

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", event.getOrderId());
        data.put("eventId", event.getEventId());
        data.put("sentTimes", times);
        data.put("hint", "同一个 eventId 只应产生一条业务通知，其余被幂等拦截");
        return ApiResult.ok(data);
    }

    /**
     * 重试演示：让消费者前 N 次强制失败，观察 RocketMQ 的重投递和最终成功。
     * 需要 application.yml 里 demo.retry.enabled=true 才会真正失败。
     */
    @PostMapping("/send-retry-demo")
    public ApiResult<Map<String, Object>> sendRetryDemo(@RequestParam(defaultValue = "2") int failTimes,
                                                        @RequestParam(required = false) String orderId) {
        OrderEvent event = demoEvent(orderId);
        event.setEventType("RETRY_DEMO");
        event.setRemark("failTimes=" + failTimes);
        producer.sendSync(MqConst.destination(MqConst.ORDER_TOPIC, MqConst.TAG_CREATED), event);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("orderId", event.getOrderId());
        data.put("eventId", event.getEventId());
        data.put("retryEnabled", retryDemoEnabled);
        data.put("hint", retryDemoEnabled
                ? "已发送，前 " + failTimes + " 次消费会失败并重投，之后成功"
                : "demo.retry.enabled=false，消息会被直接消费成功，不会触发重试");
        return ApiResult.ok(data);
    }

    /**
     * 消费结果统计，用于校验幂等是否生效。
     */
    @GetMapping("/stats")
    public ApiResult<Map<String, Object>> stats() {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("consumeRecordCount", consumeRecordRepository.countAll());
        data.put("notifyCount", notifyRepository.countAll());
        data.put("orderCount", orderService.listOrders().size());
        return ApiResult.ok(data);
    }

    private OrderEvent demoEvent(String orderId) {
        return OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(orderId == null || orderId.isEmpty() ? "DEMO-" + System.currentTimeMillis() : orderId)
                .eventType("CREATED")
                .status("CREATED")
                .userId("U-1001")
                .amount(new BigDecimal("99.00"))
                .eventTime(System.currentTimeMillis())
                .schemaVersion(1)
                .build();
    }
}
