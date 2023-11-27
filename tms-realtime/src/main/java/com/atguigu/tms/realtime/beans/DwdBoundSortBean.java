package com.atguigu.tms.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Hliang
 * @create 2023-09-30 15:02
 * 中转域:分拣实体类
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DwdBoundSortBean {
    // 编号（主键）
    String id;

    // 运单编号
    String orderId;

    // 机构id
    String orgId;

    // 分拣时间
    String sortTime;

    // 分拣人员id
    String sorterEmpId;

    // 时间戳
    Long ts;
}
