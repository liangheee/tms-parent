package com.atguigu.tms.realtime.beans;

import lombok.Builder;
import lombok.Data;

/**
 * @author Hliang
 * @create 2023-09-30 15:01
 * 中转域：入库实体类
 */
@Data
@Builder
public class DwdBoundInboundBean {
    // 编号（主键）
    String id;

    // 运单编号
    String orderId;

    // 机构id
    String orgId;

    // 入库时间
    String inboundTime;

    // 入库人员id
    String inboundEmpId;

    // 时间戳
    Long ts;
}
