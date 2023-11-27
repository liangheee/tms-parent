package com.atguigu.tms.publisher.controller;

import com.atguigu.tms.publisher.common.ResultVo;
import com.atguigu.tms.publisher.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;

/**
 * @author Hliang
 * @create 2023-10-13 21:07
 */
@RestController
public class TradeStatsController {

    @Autowired
    private TradeStatsService tradeStatsService;

    @RequestMapping("/orderAmount/{date}")
    public ResultVo getOrderAmount(@PathVariable(name = "date",required = false) String date){
        BigDecimal orderAmount = tradeStatsService.getOrderAmount(date);
        return ResultVo.succeedWithData(orderAmount);
    }

}
