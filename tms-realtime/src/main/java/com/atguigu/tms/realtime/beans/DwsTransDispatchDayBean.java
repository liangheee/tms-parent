package com.atguigu.tms.realtime.beans;

import lombok.Builder;
import lombok.Data;

/**
 * @author Hliang
 * @create 2023-10-12 21:32
 * 物流域发单统计实体类
 */
@Data
@Builder
public class DwsTransDispatchDayBean {
    // 统计日期
    String curDate;

    // 发单数
    Long dispatchOrderCountBase;

    // 时间戳
    Long ts;
}
