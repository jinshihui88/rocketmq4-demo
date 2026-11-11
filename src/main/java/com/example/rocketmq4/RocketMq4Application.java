package com.example.rocketmq4;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 启动类。
 *
 * <p>rocketmq-spring-boot-starter 通过自动配置装配 RocketMQTemplate 与监听容器，
 * 不需要额外添加 {@code @EnableRocketMQ} 之类的注解。
 */
@SpringBootApplication
public class RocketMq4Application {

    public static void main(String[] args) {
        SpringApplication.run(RocketMq4Application.class, args);
    }
}
