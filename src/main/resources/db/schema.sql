-- 初始化脚本：H2 内存库，每次启动重建
-- 生产环境请替换为 MySQL 的等价 DDL

CREATE TABLE IF NOT EXISTS t_order
(
    order_id   VARCHAR(64)    NOT NULL COMMENT '订单号',
    user_id    VARCHAR(64)    NOT NULL COMMENT '下单用户',
    amount     DECIMAL(12, 2) NOT NULL COMMENT '订单金额',
    status     VARCHAR(32)    NOT NULL COMMENT 'CREATED / PAID / CLOSED',
    created_at BIGINT         NOT NULL COMMENT '创建时间戳(ms)',
    updated_at BIGINT         NOT NULL COMMENT '更新时间戳(ms)',
    PRIMARY KEY (order_id)
);

-- 消费幂等记录表：唯一键是幂等的核心，依赖数据库约束而不是应用内存
CREATE TABLE IF NOT EXISTS mq_consume_record
(
    id             BIGINT PRIMARY KEY AUTO_INCREMENT,
    consumer_group VARCHAR(128) NOT NULL COMMENT '消费组',
    event_id       VARCHAR(64)  NOT NULL COMMENT '业务事件唯一标识',
    created_at     BIGINT       NOT NULL COMMENT '写入时间戳(ms)',
    CONSTRAINT uk_group_event UNIQUE (consumer_group, event_id)
);

-- 下游通知流水，用于直观验证"重复消息只生效一次"
CREATE TABLE IF NOT EXISTS t_order_notify
(
    id          BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id    VARCHAR(64) NOT NULL,
    event_id    VARCHAR(64) NOT NULL,
    notify_type VARCHAR(32) NOT NULL,
    created_at  BIGINT      NOT NULL
);
