package com.atguigu.tms.realtime.beans;

import com.atguigu.tms.realtime.app.annotation.TransientSink;
import lombok.Builder;
import lombok.Data;

/**
 * @author Hliang
 * @create 2023-10-01 23:09
 * 中转域：机构粒度分拣业务过程聚合统计实体类
 */
@Data
@Builder
public class DwsBoundOrgSortDayBean {
    // 统计日期
    String curDate;

    // 机构 ID
    String orgId;

    // 机构名称
    String orgName;

    // 用于关联获取省份信息的机构(转运中心的id)
    @TransientSink
    String joinOrgId;

    // 城市 ID
    String cityId;

    // 城市名称
    String cityName;

    // 省份 ID
    String provinceId;

    // 省份名称
    String provinceName;

    // 分拣次数
    Long sortCountBase;

    // 时间戳
    Long ts;
}