# rocketmq4-demo

Spring Boot 2.7.18 整合 RocketMQ 4.9.x 的完整可运行示例，配套公众号教程《Spring Boot 整合 RocketMQ 4.x 实战》。

## 1. 版本基线

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| JDK | 17 | 本机实测 17.0.12 |
| Spring Boot | 2.7.18 | 2.7 系列最后一个维护版本 |
| rocketmq-spring-boot-starter | **2.2.2** | 父 POM 依赖 rocketmq-client **4.9.3** |
| RocketMQ Broker / NameServer | 4.9.4 | 本地已启动 |
| Maven | 3.9.3 | — |

> 关键结论：`rocketmq-spring-boot-starter` **2.2.3 起父 POM 已切到 rocketmq-client 5.0.0**，
> 只有 2.2.2 及更早版本才是真正的 4.x 客户端。选 2.3.x + 4.9.x Broker 属于未验证组合。

可用 `mvn dependency:tree -Dincludes=org.apache.rocketmq` 自行核查。

## 2. 启动

```bash
# 确认 NameServer / Broker 已启动：9876 / 10911
netstat -ano | findstr :9876
netstat -ano | findstr :10911

mvn clean package -DskipTests
java -Dfile.encoding=UTF-8 -jar target/rocketmq4-demo-1.0.0.jar
```

默认端口 8090（本机 8080 已被其它项目占用，可在 `application.yml` 改）。
H2 内存库随进程启动重建，无需安装 MySQL；控制台 `/h2-console`（JDBC URL：`jdbc:h2:mem:rocketmq4`）。

## 3. 目录结构

```
src/main/java/com/example/rocketmq4
├── RocketMq4Application.java
├── common          # 统一响应、Topic/Tag/Group 常量
├── config          # 全局异常处理
├── controller      # 订单接口 + 消息能力验证接口
├── dto             # OrderEvent（消息契约）、PlaceOrderCommand
├── order           # 订单仓储、幂等记录、通知流水、状态常量
├── producer        # OrderEventProducer（5 种发送方式的统一出口）
├── service         # 订单业务、本地事务、消费处理
├── consumer        # 5 个消费者（普通/Tag 过滤/顺序/延迟/事务）
└── tx              # 事务消息监听器（本地事务 + 回查）
```

## 4. 验证接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/orders` | 下单（事务消息） |
| POST | `/api/orders/{orderId}/pay` | 支付（普通消息 + 顺序消息） |
| POST | `/api/orders/{orderId}/status-flow?count=5` | 连发 5 条状态事件，观察顺序消费 |
| GET | `/api/orders/{orderId}` | 查询订单 |
| GET | `/api/orders` | 订单列表 |
| POST | `/api/mq/send-sync?orderId=` | 同步发送 |
| POST | `/api/mq/send-async?orderId=` | 异步发送 |
| POST | `/api/mq/send-oneway?orderId=` | 单向发送 |
| POST | `/api/mq/send-delay?orderId=&delayLevel=3` | 延迟发送 |
| POST | `/api/mq/send-duplicate?times=3` | 同 eventId 连发，验证幂等 |
| POST | `/api/mq/send-retry-demo?failTimes=1` | 消费失败重试（需开启 `demo.retry.enabled`） |
| GET | `/api/mq/stats` | 消费记录 / 通知 / 订单数量统计 |

## 5. 已验证项

- 事务消息：下单落库 + 事件投递，重复提交返回 409
- 消费幂等：同一 eventId 发 3 次，业务只生效 1 次，日志出现 2 条幂等拦截
- 顺序消息：同一 orderId 的 5 条状态事件按 SEQ 1→5 顺序消费
- 延迟消息：延迟等级 3（约 10s）到点回查状态后关单
- 消费重试：第 1 次抛异常 → 10s 后重投 → 第 2 次成功
- 延迟消费/参数校验：非法参数返回 400，订单不存在返回 404

## 6. 注意

- 首次使用新 Topic 时，消费者可能等约 30 秒路由刷新才拉到消息，属正常现象；生产环境应提前建 Topic。
- `demo.retry.enabled` 默认 false，避免演示时产生无谓的重试日志。
- 消费重试用尽后进入 `%DLQ%<consumerGroup>` 死信队列，需自行监控。
