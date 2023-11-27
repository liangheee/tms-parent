package com.atguigu.tms.realtime.beans;

import com.atguigu.tms.realtime.utils.DateFormatUtil;
import lombok.Data;

import java.math.BigDecimal;

/**
 * @author Hliang
 * @create 2023-09-30 11:38
 *交易域:下单事务事实表实体类
 */
@Data
public class DwdTradeOrderDetailBean {
    // 运单明细ID
    String id;

    // 运单id
    String orderId;

    // 货物类型
    String cargoType;

    // 长cm
    Integer volumeLength;

    // 宽cm
    Integer volumeWidth;

    // 高cm
    Integer volumeHeight;

    // 重量 kg
    BigDecimal weight;

    // 下单时间
    String orderTime;

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
    String estimateArriveTime;

    // 距离，单位：公里
    BigDecimal distance;

    // 时间戳
    Long ts;

    public void mergeBean(DwdOrderDetailOriginBean detailOriginBean, DwdOrderInfoOriginBean infoOriginBean) {
        // 合并原始明细字段
        this.id = detailOriginBean.id;
        this.orderId = detailOriginBean.orderId;
        this.cargoType = detailOriginBean.cargoType;
        this.volumeLength = detailOriginBean.volumnLength;
        this.volumeWidth = detailOriginBean.volumnWidth;
        this.volumeHeight = detailOriginBean.volumnHeight;
        this.weight = detailOriginBean.weight;
        this.orderTime =
                DateFormatUtil.toYmdHms(DateFormatUtil.toTs(
                        detailOriginBean.createTime.replaceAll("T", " ")
                                .replaceAll("Z", ""), true)
                        + 8 * 60 * 60 * 1000);
        this.ts = DateFormatUtil.toTs(
                detailOriginBean.createTime.replaceAll("T", " ")
                        .replaceAll("Z", ""), true)
                + 8 * 60 * 60 * 1000;

        // 合并原始订单字段
        this.orderNo = infoOriginBean.orderNo;
        this.status = infoOriginBean.status;
        this.collectType = infoOriginBean.collectType;
        this.userId = infoOriginBean.userId;
        this.receiverComplexId = infoOriginBean.receiverComplexId;
        this.receiverProvinceId = infoOriginBean.receiverProvinceId;
        this.receiverCityId = infoOriginBean.receiverCityId;
        this.receiverDistrictId = infoOriginBean.receiverDistrictId;
        this.receiverName = infoOriginBean.receiverName;
        this.senderComplexId = infoOriginBean.senderComplexId;
        this.senderProvinceId = infoOriginBean.senderProvinceId;
        this.senderCityId = infoOriginBean.senderCityId;
        this.senderDistrictId = infoOriginBean.senderDistrictId;
        this.senderName = infoOriginBean.senderName;
        this.paymentType = infoOriginBean.paymentType;
        this.cargoNum = infoOriginBean.cargoNum;
        this.amount = infoOriginBean.amount;
        this.estimateArriveTime = DateFormatUtil.toYmdHms(
                infoOriginBean.estimateArriveTime - 8 * 60 * 60 * 1000);
        this.distance = infoOriginBean.distance;
    }
}
