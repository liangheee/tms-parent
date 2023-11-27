package com.atguigu.tms.realtime.beans;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author Hliang
 * @create 2023-09-30 14:13
 *物流域:运输完成事实表实体类
 */
@Data
public class DwdTransTransFinishBean {
    // 编号（主键）
    String id;

    // 班次ID
    String shiftId;

    // 线路ID
    String lineId;

    // 起始机构ID
    String startOrgId;

    // 起始机构名称
    String startOrgName;

    // 目的机构id
    String endOrgId;

    // 目的机构名称
    String endOrgName;

    // 运单个数
    Integer orderNum;

    // 司机1 ID
    String driver1EmpId;

    // 司机1名称
    String driver1Name;

    // 司机2 ID
    String driver2EmpId;

    // 司机2名称
    String driver2Name;

    // 卡车ID
    String truckId;

    // 卡车号牌
    String truckNo;

    // 实际启动时间
    String actualStartTime;

    // 实际到达时间
    String actualEndTime;

    // 运输时长
    Long transportTime;

    // 实际行驶距离
    BigDecimal actualDistance;

    // 时间戳
    Long ts;
}
