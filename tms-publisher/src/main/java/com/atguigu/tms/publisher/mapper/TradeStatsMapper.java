package com.atguigu.tms.publisher.mapper;

import org.apache.ibatis.annotations.Param;

import java.math.BigDecimal;

/**
 * @author Hliang
 * @create 2023-10-13 8:30
 */
public interface TradeStatsMapper {
    // 获取某天下单总金额
    BigDecimal selectOrderAmount(@Param("date") String date);
}
