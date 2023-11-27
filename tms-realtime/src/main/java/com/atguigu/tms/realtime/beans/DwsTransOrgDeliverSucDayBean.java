package com.atguigu.tms.realtime.beans;

import com.atguigu.tms.realtime.app.annotation.TransientSink;
import lombok.Builder;
import lombok.Data;

/**
 * @author Hliang
 * @create 2023-10-12 21:52
 * 物流域机构派送成功统计实体类
 */
@Data
@Builder
public class DwsTransOrgDeliverSucDayBean {
    // 统计日期
    String curDate;

    // 机构 ID
    String orgId;

    // 机构名称
    String orgName;

    // 地区 ID
    @TransientSink
    String districtId;

    // 城市 ID
    String cityId;

    // 城市名称
    String cityName;

    // 省份 ID
    String provinceId;

    // 省份名称
    String provinceName;

    // 派送成功次数
    Long deliverSucCountBase;

    // 时间戳
    Long ts;
}
