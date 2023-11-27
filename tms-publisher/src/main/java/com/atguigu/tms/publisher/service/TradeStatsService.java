package com.atguigu.tms.publisher.service;

import java.math.BigDecimal;

/**
 * @author Hliang
 * @create 2023-10-13 21:16
 */
public interface TradeStatsService {
    BigDecimal getOrderAmount(String date);
}
