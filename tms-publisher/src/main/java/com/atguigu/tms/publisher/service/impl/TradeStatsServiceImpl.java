package com.atguigu.tms.publisher.service.impl;

import com.atguigu.tms.publisher.mapper.TradeStatsMapper;
import com.atguigu.tms.publisher.service.TradeStatsService;
import com.atguigu.tms.publisher.utils.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * @author Hliang
 * @create 2023-10-13 21:17
 */
@Service
public class TradeStatsServiceImpl implements TradeStatsService {

    @Autowired
    private TradeStatsMapper tradeStatsMapper;

    @Override
    public BigDecimal getOrderAmount(String date) {
        if(StringUtils.isEmpty(date)){
            date = DateFormatUtil.now();
        }
        return tradeStatsMapper.selectOrderAmount(date);
    }
}
