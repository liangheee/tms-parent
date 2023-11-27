package com.atguigu.tms.realtime.beans;

import lombok.Builder;
import lombok.Data;

/**
 * @author Hliang
 * @create 2023-10-12 20:50
 * 物流域转运完成实体类
 */
@Data
@Builder
public class DwsTransBoundFinishDayBean {
    // 统计日期
    String curDate;

    // 转运完成次数
    Long boundFinishOrderCountBase;

    // 时间戳
    Long ts;
}