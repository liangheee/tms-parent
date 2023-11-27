package com.atguigu.tms.realtime.beans;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author Hliang
 * @create 2023-09-30 11:37
 *订单货物明细实体类
 */
@Data
public class DwdOrderDetailOriginBean {
    // 编号（主键）
    String id;

    // 运单id
    String orderId;

    // 货物类型
    String cargoType;

    // 长cm
    Integer volumnLength;

    // 宽cm
    Integer volumnWidth;

    // 高cm
    Integer volumnHeight;

    // 重量 kg
    BigDecimal weight;

    // 创建时间
    String createTime;

    // 更新时间
    String updateTime;

    // 是否删除
    String isDeleted;

}
