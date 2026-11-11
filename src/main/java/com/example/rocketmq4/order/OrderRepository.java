package com.example.rocketmq4.order;

import com.example.rocketmq4.dto.PlaceOrderCommand;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * 订单仓储。所有状态变更都是「带前置状态的条件更新」，
 * 返回受影响行数，由调用方判断是否真的发生了流转。
 */
@Repository
public class OrderRepository {

    private static final String COLUMNS = "order_id, user_id, amount, status, created_at, updated_at";

    private static final RowMapper<OrderRecord> ROW_MAPPER = (rs, rowNum) -> new OrderRecord(
            rs.getString("order_id"),
            rs.getString("user_id"),
            rs.getBigDecimal("amount"),
            rs.getString("status"),
            rs.getLong("created_at"),
            rs.getLong("updated_at")
    );

    private final JdbcTemplate jdbcTemplate;

    public OrderRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 新增订单。order_id 是主键，重复插入会抛 DuplicateKeyException，
     * 这正是「同一笔业务重复提交」的天然幂等保护。
     */
    public void insert(PlaceOrderCommand command, String status, long now) {
        jdbcTemplate.update(
                "INSERT INTO t_order (order_id, user_id, amount, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                command.getOrderId(), command.getUserId(), command.getAmount(), status, now, now
        );
    }

    public boolean exists(String orderId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM t_order WHERE order_id = ?", Integer.class, orderId);
        return count != null && count > 0;
    }

    public Optional<OrderRecord> findById(String orderId) {
        List<OrderRecord> list = jdbcTemplate.query(
                "SELECT " + COLUMNS + " FROM t_order WHERE order_id = ?", ROW_MAPPER, orderId);
        return list.stream().findFirst();
    }

    public List<OrderRecord> findAll() {
        return jdbcTemplate.query("SELECT " + COLUMNS + " FROM t_order ORDER BY created_at DESC", ROW_MAPPER);
    }

    /**
     * 条件更新状态：只有当前状态等于 expectedStatus 时才更新。
     *
     * @return 受影响行数，0 表示状态已被其他请求改变，本次无需处理
     */
    public int updateStatus(String orderId, String expectedStatus, String targetStatus, long now) {
        return jdbcTemplate.update(
                "UPDATE t_order SET status = ?, updated_at = ? WHERE order_id = ? AND status = ?",
                targetStatus, now, orderId, expectedStatus
        );
    }
}
