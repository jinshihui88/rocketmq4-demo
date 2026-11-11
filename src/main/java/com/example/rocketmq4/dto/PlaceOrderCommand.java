package com.example.rocketmq4.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

/**
 * 下单请求。orderId 由调用方传入，便于重复提交时快速识别同一笔业务。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PlaceOrderCommand {

    @NotBlank(message = "orderId 不能为空")
    private String orderId;

    @NotBlank(message = "userId 不能为空")
    private String userId;

    @NotNull(message = "amount 不能为空")
    @DecimalMin(value = "0.01", message = "amount 必须大于 0")
    private BigDecimal amount;
}
