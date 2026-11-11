package com.example.rocketmq4.common;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 统一响应体。
 *
 * @param <T> 业务数据类型
 */
@Data
@AllArgsConstructor
public class ApiResult<T> {

    private int code;
    private String message;
    private T data;

    public static <T> ApiResult<T> ok(T data) {
        return new ApiResult<>(0, "ok", data);
    }

    public static <T> ApiResult<T> ok() {
        return new ApiResult<>(0, "ok", null);
    }

    public static <T> ApiResult<T> fail(int code, String message) {
        return new ApiResult<>(code, message, null);
    }
}
