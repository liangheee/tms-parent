package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.beans.DwdBoundInboundBean;
import com.atguigu.tms.realtime.beans.DwdBoundOutboundBean;
import com.atguigu.tms.realtime.beans.DwdBoundSortBean;
import com.atguigu.tms.realtime.beans.DwdOrderOrgBoundOriginBean;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author Hliang
 * @create 2023-09-30 15:02
 */
public class DwdBoundRelevantApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、环境准备
        // 1.1 创建流处理环境、设置检查点
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        // 1.2 设置并行度
        env.setParallelism(4);
        // TODO 2、读取主流数据
        String topic = "tms01_ods";
        String groupId = "bound_kafka_source";
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> odsStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("bound_kafka_source");

        // TODO 3、过滤出order_org_bound表数据
        SingleOutputStreamOperator<String> boundStrDS = odsStrDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String table = jsonObj.getJSONObject("source").getString("table");
                return "order_org_bound".equals(table);
            }
        });

        // TODO 4、转换结构为通用结构，并进行简单的ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = boundStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                jsonObj.remove("source");
                jsonObj.remove("ts");
                return jsonObj;
            }
        });

        // TODO 5、定义侧输出流标签
        //分拣侧输出流标签
        OutputTag<String> sortTag = new OutputTag<String>("sortTag") {};
        //出库侧输出流标签
        OutputTag<String> outboundTag = new OutputTag<String>("outboundTag") {};

        // TODO 6、对流数据进行分流
        SingleOutputStreamOperator<String> inboundDS = jsonObjDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<String> out) throws Exception {
                DwdOrderOrgBoundOriginBean beforeBoundOriginBean = jsonObj.getObject("before", DwdOrderOrgBoundOriginBean.class);
                DwdOrderOrgBoundOriginBean afterBoundOriginBean = jsonObj.getObject("after", DwdOrderOrgBoundOriginBean.class);
                String op = jsonObj.getString("op");

                String id = afterBoundOriginBean.getId();
                String orderId = afterBoundOriginBean.getOrderId();
                String orgId = afterBoundOriginBean.getOrgId();

                if ("c".equals(op)) {
                    // 表示当前是入库操作
                    long inboundTime = Long.parseLong(afterBoundOriginBean.getInboundTime()) - 8 * 60 * 60 * 1000;
                    DwdBoundInboundBean dwdBoundInboundBean = DwdBoundInboundBean.builder()
                            .id(id)
                            .orderId(orderId)
                            .orgId(orgId)
                            .inboundTime(DateFormatUtil.toYmdHms(inboundTime))
                            .inboundEmpId(afterBoundOriginBean.getInboundEmpId())
                            .ts(inboundTime)
                            .build();
                    out.collect(JSON.toJSONString(dwdBoundInboundBean));
                } else if ("u".equals(op)) {
                    String beforeSortTime = beforeBoundOriginBean.getSortTime();
                    String afterSortTime = afterBoundOriginBean.getSortTime();
                    String beforeOutBoundTime = beforeBoundOriginBean.getOutboundTime();
                    String afterOutboundTime = afterBoundOriginBean.getOutboundTime();

                    if (beforeSortTime == null && afterSortTime != null) {
                        // 分拣操作
                        DwdBoundSortBean dwdBoundSortBean = DwdBoundSortBean.builder()
                                .id(id)
                                .orderId(orderId)
                                .orgId(orgId)
                                .sortTime(DateFormatUtil.toYmdHms(Long.parseLong(afterSortTime) - 8 * 60 * 60 * 1000))
                                .sorterEmpId(afterBoundOriginBean.getSorterEmpId())
                                .ts(Long.parseLong(afterSortTime) - 8 * 60 * 60 * 1000)
                                .build();
                        ctx.output(sortTag, JSON.toJSONString(dwdBoundSortBean));
                    }

                    if (beforeOutBoundTime == null && afterOutboundTime != null) {
                        // 出库操作
                        DwdBoundOutboundBean dwdBoundOutboundBean = DwdBoundOutboundBean.builder()
                                .id(id)
                                .orderId(orderId)
                                .orgId(orgId)
                                .outboundTime(DateFormatUtil.toYmdHms(Long.parseLong(afterOutboundTime) - 8 * 60 * 60 * 1000))
                                .outboundEmpId(afterBoundOriginBean.getOutboundEmpId())
                                .ts(Long.parseLong(afterOutboundTime) - 8 * 60 * 60 * 1000)
                                .build();
                        ctx.output(outboundTag, JSON.toJSONString(dwdBoundOutboundBean));
                    }
                }
            }
        }).uid("process_data");

        // TODO 7、获取不同的分流
        SideOutputDataStream<String> sortDS = inboundDS.getSideOutput(sortTag);
        SideOutputDataStream<String> outboundDS = inboundDS.getSideOutput(outboundTag);

        // TODO 8、写出到Kafka对应主题
        //中转域入库事实主题
        String inboundTopic = "tms_dwd_bound_inbound";
        //中转域分拣事实主题
        String sortTopic = "tms_dwd_bound_sort";
        //中转域出库事实主题
        String outboundTopic = "tms_dwd_bound_outbound";

        KafkaSink<String> inboundKafkaSink = KafkaUtil.createKafkaSink(args, inboundTopic, inboundTopic + "_trans");
        inboundDS.print(">>>");
        inboundDS.sinkTo(inboundKafkaSink).uid("inbound_sink");

        KafkaSink<String> sortKafkaSink = KafkaUtil.createKafkaSink(args, sortTopic, sortTopic + "_trans");
        sortDS.print("@@@");
        sortDS.sinkTo(sortKafkaSink).uid("sort_sink");

        KafkaSink<String> outbountKafkaSink = KafkaUtil.createKafkaSink(args, outboundTopic, outboundTopic + "_trans");
        outboundDS.print("###");
        outboundDS.sinkTo(outbountKafkaSink).uid("outbound_sink");

        // TODO 9、执行Flink应用
        env.execute();
    }
}
