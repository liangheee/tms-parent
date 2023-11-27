package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.beans.*;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Hliang
 * @create 2023-09-27 23:46
 */
public class DwdOrderRelevantApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、环境准备
        // 1.1 创建流处理环境、设置检查点
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        // 1.2 设置并行度
        env.setParallelism(4);

        // TODO 2、读取主流数据
        // 2.1 定义消费主题
        String topic = "tms01_ods";
        // 2.2 定义消费者组id
        String groupId = "ods_kafka_consumer";
        // 2.3 消费数据
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> odsKafkaStrDS = env.
                fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3、过滤出主流中关于OrderInfo和OrderCargo的数据
        SingleOutputStreamOperator<String> orderStrDS = odsKafkaStrDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                String table = jsonObj.getJSONObject("source").getString("table");
                return "order_info".equals(table) || "order_cargo".equals(table);
            }
        });

        // TODO 4、转换数据结构为通用对象JsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = orderStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                // ETL清洗掉一些非必要数据
                String table = jsonObj.getJSONObject("source").getString("table");
                jsonObj.put("table", table);
                jsonObj.remove("source");
                jsonObj.remove("ts");
                return jsonObj;
            }
        });

        // TODO 5、将过滤出的结果进行keyBy，使得相同OrderId的数据进入一个分区
        KeyedStream<JSONObject, String> keyedJsonObjDS = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObj) throws Exception {
                String table = jsonObj.getString("table");
                if ("order_info".equals(table)) {
                    return jsonObj.getJSONObject("after").getString("id");
                } else {
                    return jsonObj.getJSONObject("after").getString("order_id");
                }
            }
        });

//        keyedJsonObjDS.print(">>>");
        // TODO 6、定义侧输出流标签 (new OutputTag存在泛型擦除，可以通过写匿名字类或者Types指定类型解决问题)
        // 支付成功明细流标签
        OutputTag<String> paySucTag = new OutputTag<String>("dwd_trade_pay_suc_detail") {};
        // 取消运单明细流标签
        OutputTag<String> cancelDetailTag = new OutputTag<String>("dwd_trade_cancel_detail") {};
        // 揽收明细流标签
        OutputTag<String> receiveDetailTag = new OutputTag<String>("dwd_trans_receive_detail") {};
        // 发单明细流标签
        OutputTag<String> dispatchDetailTag = new OutputTag<String>("dwd_trans_dispatch_detail") {};
        // 转运完成明细流标签
        OutputTag<String> boundFinishDetailTag = new OutputTag<String>("dwd_trans_bound_finish_detail") {};
        // 派送成功明细流标签
        OutputTag<String> deliverSucDetailTag = new OutputTag<String>("dwd_trans_deliver_detail") {};
        // 签收明细流标签
        OutputTag<String> signDetailTag = new OutputTag<String>("dwd_trans_sign_detail") {};

        // TODO 7、分流：对OrderInfo和OrderCargo数据进行关联（不能采用Join或者IntervalJoin，因为窗口大小我们是不确定的），关联后写入相应的侧输出流
        //         业务场景：
        //                  用户下单寄件，就会在order_info和order_cargo中各自添加一条数据（op=c）
        //                  之后，物流的一系列过程，仅仅会修改order_info的状态，而order_cargo不会再发生改变
        //                  但是order_info的状态具体什么时候改变，我们是无法确定的
        //                  因此，我们不能使用传统的Join或者IntervalJoin去关联order_info和order_cargo，确定不了窗口时间或者上下的interval
        //                  换一个思路，我们要关联order_info和order_cargo，由于二者在流中的顺序，谁先来，谁后来不确定，我们必须要把先来的数据保存起来
        //                  那么这样就可以使得后来的数据可以去保存的数据里面寻找，然后进行关联操作，自然而然我们想到Flink中的状态
        //                  而且这里是对orderId相同的数据进行关联，我们前面做了keyBy，所以这里就要用到键控状态ValueState
        //                  因为用户一下单，就会往order_info和order_cargo中各自添加一条数据，此时的操作op=c，他们两个会先后通过Flink-CDC同步到Kafka的tms01_ods中
        //                  但是他们谁先来谁后来我们是无法确定的，所以对于op=c的操作，无论二者谁先来，我们都需要缓存起来，避免数据丢书，导致状态不一致
        //                  当一条数据来的时候，就去相应的状态里面寻找进行关联，不过二者的先后到来时间一般不会太久，而且order_info的op=c的数据关联后就可以删除，但是order_cargo不能
        //                  因为order_cargo的数据还要和之后order_info的状态改变的数据进行关联，所以对于order_info的op=c的数据的状态存储，我们可以设置ttl为5s
        //                  5s以后让其自动清除，而对于order_cargo的数据的状态我们就需要手动清除，因为他什么时候关联结束，不需要它了，时间是不确定的
        //                  不过这里，我觉得order_info的op=c的数据的状态也可以在我们关联order_cargo后进行手动删除，而且这样更加确定其能够关联上order_cargo
        SingleOutputStreamOperator<String> orderDetailDS = keyedJsonObjDS.process(new KeyedProcessFunction<String, JSONObject, String>() {

            // 定义键控状态
            ValueState<DwdOrderInfoOriginBean> orderInfoBeanState;
            ValueState<DwdOrderDetailOriginBean> orderDetailBeanState;

            // 初始化键控状态
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<DwdOrderInfoOriginBean> orderInfoOriginBeanValueStateDescriptor = new ValueStateDescriptor<>("orderInfoValueState", Types.POJO(DwdOrderInfoOriginBean.class));
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(5)).build();
                orderInfoOriginBeanValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                orderInfoBeanState = getRuntimeContext().getState(orderInfoOriginBeanValueStateDescriptor);

                ValueStateDescriptor<DwdOrderDetailOriginBean> orderDetailValueStateDescriptor = new ValueStateDescriptor<>("orderDetailValueState", Types.POJO(DwdOrderDetailOriginBean.class));
                orderDetailBeanState = getRuntimeContext().getState(orderDetailValueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                String table = jsonObj.getString("table");
                // order_info和order_cargo分别处理
                if ("order_info".equals(table)) {
                    DwdOrderInfoOriginBean orderInfoOriginBean = jsonObj.getObject("after", DwdOrderInfoOriginBean.class);
                    // 脱敏
                    String senderName = orderInfoOriginBean.getSenderName();
                    String receiverName = orderInfoOriginBean.getReceiverName();

                    senderName = senderName.charAt(0) + senderName.substring(1).replaceAll(".", "\\*");
                    receiverName = receiverName.charAt(0) + receiverName.substring(1).replaceAll(".", "\\*");

                    orderInfoOriginBean.setSenderName(senderName);
                    orderInfoOriginBean.setReceiverName(receiverName);

                    // 获取orderDetailBeanState中的orderCargo
                    DwdOrderDetailOriginBean orderDetailOriginBean = orderDetailBeanState.value();

                    // 如果op = c，那么就是刚刚下单
                    if ("c".equals(jsonObj.getString("op"))) {
                        // 如果orderDetail状态不为空，立马关联
                        if (orderDetailOriginBean != null) {
                            DwdTradeOrderDetailBean dwdTradeOrderDetailBean = new DwdTradeOrderDetailBean();
                            dwdTradeOrderDetailBean.mergeBean(orderDetailOriginBean, orderInfoOriginBean);
                            // 写出到主流
                            out.collect(JSON.toJSONString(dwdTradeOrderDetailBean));
                        } else {
                            // 如果orderDetail状态为空，则将OrderInfo放入状态中，便于后续关联
                            orderInfoBeanState.update(orderInfoOriginBean);
                        }

                    } else if ("u".equals(jsonObj.getString("op")) && orderDetailOriginBean != null) {
                        DwdOrderInfoOriginBean beforeOrderInfoBean = jsonObj.getObject("before", DwdOrderInfoOriginBean.class);
                        String changeStatus = beforeOrderInfoBean.getStatus() + " -> " + orderInfoOriginBean.getStatus();

                        switch (changeStatus) {
                            case "60010 -> 60020":
                                // 支付成功
                                DwdTradePaySucDetailBean dwdTradePaySucDetailBean = new DwdTradePaySucDetailBean();
                                dwdTradePaySucDetailBean.mergeBean(orderDetailOriginBean, orderInfoOriginBean);
                                // 写入侧输出流
                                ctx.output(paySucTag, JSON.toJSONString(dwdTradePaySucDetailBean));
                                break;
                            case "60020 -> 60030":
                                // 揽收
                                DwdTransReceiveDetailBean dwdTransReceiveDetailBean = new DwdTransReceiveDetailBean();
                                dwdTransReceiveDetailBean.mergeBean(orderDetailOriginBean, orderInfoOriginBean);
                                // 写入侧输出流
                                ctx.output(receiveDetailTag, JSON.toJSONString(dwdTransReceiveDetailBean));
                                break;
                            case "60040 -> 60050":
                                // 发单
                                DwdTransDispatchDetailBean dwdTransDispatchDetailBean = new DwdTransDispatchDetailBean();
                                dwdTransDispatchDetailBean.mergeBean(orderDetailOriginBean, orderInfoOriginBean);
                                // 写入侧输出流
                                ctx.output(dispatchDetailTag, JSON.toJSONString(dwdTransDispatchDetailBean));
                                break;
                            case "60050 -> 60060":
                                // 转运完成
                                DwdTransBoundFinishDetailBean dwdTransBoundFinishDetailBean = new DwdTransBoundFinishDetailBean();
                                dwdTransBoundFinishDetailBean.mergeBean(orderDetailOriginBean, orderInfoOriginBean);
                                // 写入侧输出流
                                ctx.output(boundFinishDetailTag, JSON.toJSONString(dwdTransBoundFinishDetailBean));
                                break;
                            case "60060 -> 60070":
                                // 派送成功
                                DwdTransDeliverSucDetailBean dwdTransDeliverSucDetailBean = new DwdTransDeliverSucDetailBean();
                                dwdTransDeliverSucDetailBean.mergeBean(orderDetailOriginBean, orderInfoOriginBean);
                                // 写入侧输出流
                                ctx.output(deliverSucDetailTag, JSON.toJSONString(dwdTransDeliverSucDetailBean));
                                break;
                            case "60070 -> 60080":
                                // 签收
                                DwdTransSignDetailBean dwdTransSignDetailBean = new DwdTransSignDetailBean();
                                dwdTransSignDetailBean.mergeBean(orderDetailOriginBean, orderInfoOriginBean);
                                // 写入侧输出流
                                ctx.output(signDetailTag, JSON.toJSONString(dwdTransSignDetailBean));
                                break;
                            default:
                                // 其它 —> 60999
                                // 取消订单
                                if ("60999".equals(orderInfoOriginBean.getStatus())) {
                                    DwdTradeCancelDetailBean dwdTradeCancelDetailBean = new DwdTradeCancelDetailBean();
                                    dwdTradeCancelDetailBean.mergeBean(orderDetailOriginBean, orderInfoOriginBean);
                                    // 写入侧输出流
                                    ctx.output(cancelDetailTag, JSON.toJSONString(dwdTradeCancelDetailBean));
                                    // TODO 手动删除orderDetailBeanSate！！！
                                    orderDetailBeanState.clear();
                                }
                        }
                    }
                } else if("order_cargo".equals(table)) {
                    // 处理orderDetail
                    DwdOrderDetailOriginBean dwdOrderDetailOriginBean = jsonObj.getObject("after", DwdOrderDetailOriginBean.class);
                    // 保存orderDetail状态
                    orderDetailBeanState.update(dwdOrderDetailOriginBean);

                    // 判断当前orderInfoBeanState是否有数据，有数据立马关联
                    if (orderInfoBeanState.value() != null) {
                        DwdTradeOrderDetailBean dwdTradeOrderDetailBean = new DwdTradeOrderDetailBean();
                        dwdTradeOrderDetailBean.mergeBean(dwdOrderDetailOriginBean, orderInfoBeanState.value());
                        // 写入主流
                        out.collect(JSON.toJSONString(dwdTradeOrderDetailBean));
                    }
                }
            }
        }).uid("process_data");

        // TODO 8、获取不同的分流
        // 定义主题
        //8.1 支付成功明细流
        DataStream<String> paySucDS = orderDetailDS.getSideOutput(paySucTag);
        // 8.2 取消运单明细流
        DataStream<String> cancelDetailDS = orderDetailDS.getSideOutput(cancelDetailTag);
        // 8.3 揽收明细流
        DataStream<String> receiveDetailDS = orderDetailDS.getSideOutput(receiveDetailTag);
        // 8.4 发单明细流
        DataStream<String> dispatchDetailDS = orderDetailDS.getSideOutput(dispatchDetailTag);
        // 8.5 转运成功明细流
        DataStream<String> boundFinishDetailDS = orderDetailDS.getSideOutput(boundFinishDetailTag);
        // 8.6 派送成功明细流
        DataStream<String> deliverSucDetailDS = orderDetailDS.getSideOutput(deliverSucDetailTag);
        // 8.7 签收明细流
        DataStream<String> signDetailDS = orderDetailDS.getSideOutput(signDetailTag);

        // TODO 9、写出到kafka对应主题
        //9.1 定义主题名称
        // 9.1.1 交易域下单明细主题
        String detailTopic = "tms_dwd_trade_order_detail";
        // 9.1.2 交易域支付成功明细主题
        String paySucDetailTopic = "tms_dwd_trade_pay_suc_detail";
        // 9.1.3 交易域取消运单明细主题
        String cancelDetailTopic = "tms_dwd_trade_cancel_detail";
        // 9.1.4 物流域接单（揽收）明细主题
        String receiveDetailTopic = "tms_dwd_trans_receive_detail";
        // 9.1.5 物流域发单明细主题
        String dispatchDetailTopic = "tms_dwd_trans_dispatch_detail";
        // 9.1.6 物流域转运完成明细主题
        String boundFinishDetailTopic = "tms_dwd_trans_bound_finish_detail";
        // 9.1.7 物流域派送成功明细主题
        String deliverSucDetailTopic = "tms_dwd_trans_deliver_detail";
        // 9.1.8 物流域签收明细主题
        String signDetailTopic = "tms_dwd_trans_sign_detail";

        // 9.2 发送数据到 Kafka
        // 9.2.1 运单明细数据
        KafkaSink<String> detailKafkaSink = KafkaUtil.createKafkaSink(args, detailTopic, detailTopic + "_trans");
        orderDetailDS.print("~~");
        orderDetailDS.sinkTo(detailKafkaSink).uid("order_detail_sink");

        // 9.2.2 支付成功明细数据
        KafkaSink<String> paySucKafkaSink = KafkaUtil.createKafkaSink(args, paySucDetailTopic, paySucDetailTopic + "_trans");
        paySucDS.print("!!");
        paySucDS.sinkTo(paySucKafkaSink).uid("pay_suc_detail_sink");

        // 9.2.3 取消运单明细数据
        KafkaSink<String> cancelDetailKafkaSink = KafkaUtil.createKafkaSink(args, cancelDetailTopic, cancelDetailTopic + "_trans");
        cancelDetailDS.print("@@");
        cancelDetailDS.sinkTo(cancelDetailKafkaSink).uid("cancel_detail_sink");

        // 9.2.4 揽收明细数据
        KafkaSink<String> receiveDetailKafkaSink = KafkaUtil.createKafkaSink(args, receiveDetailTopic, receiveDetailTopic + "_trans");
        receiveDetailDS.print("##");
        receiveDetailDS.sinkTo(receiveDetailKafkaSink).uid("receive_detail_sink");

        // 9.2.5 发单明细数据
        KafkaSink<String> dispatchDetailKafkaSink = KafkaUtil.createKafkaSink(args, dispatchDetailTopic, dispatchDetailTopic + "_trans");
        dispatchDetailDS.print("$$");
        dispatchDetailDS.sinkTo(dispatchDetailKafkaSink).uid("dispatch_detail_sink");

        // 9.2.6 转运完成明细数据
        KafkaSink<String> boundFinishDetailKafkaSink = KafkaUtil.createKafkaSink(args, boundFinishDetailTopic, boundFinishDetailTopic + "_trans");
        boundFinishDetailDS.print("%%");
        boundFinishDetailDS.sinkTo(boundFinishDetailKafkaSink).uid("bound_finish_detail_sink");

        // 9.2.7 派送成功明细数据
        KafkaSink<String> deliverySucKafkaSink = KafkaUtil.createKafkaSink(args, deliverSucDetailTopic, deliverSucDetailTopic + "_trans");
        deliverSucDetailDS.print("^^");
        deliverSucDetailDS.sinkTo(deliverySucKafkaSink).uid("delivery_suc_sink");

        // 9.2.8 签收明细数据
        KafkaSink<String> signDetailKafkaSink = KafkaUtil.createKafkaSink(args, signDetailTopic, signDetailTopic + "_trans");
        signDetailDS.print("&&");
        signDetailDS.sinkTo(signDetailKafkaSink).uid("sign_detail_sink");

        // TODO 10、执行Flink
        env.execute();
    }
}
