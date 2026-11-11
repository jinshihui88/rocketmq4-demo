package com.example.rocketmq4.order;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 消费幂等记录仓储。
 *
 * <p>幂等依靠数据库唯一键 {@code uk_group_event}，而不是应用内存或短 TTL 的 Redis 锁：
 * 进程重启、扩容多实例、Redis 过期都不会让幂等失效。
 */
@Repository
public class ConsumeRecordRepository {

    private final JdbcTemplate jdbcTemplate;

    public ConsumeRecordRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 尝试登记一次消费。
     *
     * @return true 表示首次消费，可以继续业务处理；false 表示重复消息
     */
    public boolean tryInsert(String consumerGroup, String eventId, long now) {
        try {
            jdbcTemplate.update(
                    "INSERT INTO mq_consume_record (consumer_group, event_id, created_at) VALUES (?, ?, ?)",
                    consumerGroup, eventId, now
            );
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    public int countAll() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM mq_consume_record", Integer.class);
        return count == null ? 0 : count;
    }
}
