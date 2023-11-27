package com.atguigu.tms.realtime.beans;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author Hliang
 * @create 2023-09-30 11:38
 * 订单实体类
 */
@Data
public class DwdOrderInfoOriginBean {
    // 编号（主键）
    String id;

    // 运单号
    String orderNo;

    // 运单状态
    String status;

    // 取件类型，1为网点自寄，2为上门取件
    String collectType;

    // 客户id
    String userId;

    // 收件人小区id
    String receiverComplexId;

    // 收件人省份id
    String receiverProvinceId;

    // 收件人城市id
    String receiverCityId;

    // 收件人区县id
    String receiverDistrictId;

    // 收件人姓名
    String receiverName;

    // 发件人小区id
    String senderComplexId;

    // 发件人省份id
    String senderProvinceId;

    // 发件人城市id
    String senderCityId;

    // 发件人区县id
    String senderDistrictId;

    // 发件人姓名
    String senderName;

    // 支付方式
    String paymentType;

    // 货物个数
    Integer cargoNum;

    // 金额
    BigDecimal amount;

    // 预计到达时间
    Long estimateArriveTime;

    // 距离，单位：公里
    BigDecimal distance;

    // 创建时间
    String createTime;

    // 更新时间
    String updateTime;

    // 是否删除
    String isDeleted;
}

