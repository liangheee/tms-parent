package com.atguigu.tms.realtime.beans;

import com.atguigu.tms.realtime.app.annotation.TransientSink;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * @author Hliang
 * @create 2023-10-12 19:43
 * 交易域机构粒度下单聚合统计实体类
 */
@Data
@Builder
public class DwsTradeOrgOrderDayBean {

    // 日期
    String curDate;

    // 机构ID
    String orgId;

    // 机构名称
    String orgName;

    // 城市ID
    String cityId;

    // 城市名称
    String cityName;

    // 发货人区县ID
    @TransientSink
    String senderDistrictId;

    // 下单金额
    BigDecimal orderAmountBase;

    // 下单次数
    Long orderCountBase;

    // 时间戳
    Long ts;
}
