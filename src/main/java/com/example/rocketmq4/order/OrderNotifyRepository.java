package com.example.rocketmq4.order;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 下游通知流水，用来直观验证「同一条消息只产生一次业务效果」。
 */
@Repository
public class OrderNotifyRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderNotifyRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(String orderId, String eventId, String notifyType, long now) {
        jdbcTemplate.update(
                "INSERT INTO t_order_notify (order_id, event_id, notify_type, created_at) VALUES (?, ?, ?, ?)",
                orderId, eventId, notifyType, now
        );
    }

    public int countByOrderId(String orderId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM t_order_notify WHERE order_id = ?", Integer.class, orderId);
        return count == null ? 0 : count;
    }

    public int countAll() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM t_order_notify", Integer.class);
        return count == null ? 0 : count;
    }
}
