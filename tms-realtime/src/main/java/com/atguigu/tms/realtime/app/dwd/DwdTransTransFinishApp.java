package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.beans.DwdTransTransFinishBean;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-09-30 13:45
 */
public class DwdTransTransFinishApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、环境准备
        // 1.1 创建流处理环境，设置检查点
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        // 1.2 设置并行度
        env.setParallelism(4);
        // TODO 2、读取主流数据
        String topic = "tms01_ods";
        String groupId = "ods_kafka_source";
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> odsKafkaDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3、过滤出transport_task运输完成流数据
        SingleOutputStreamOperator<String> transportTaskDS = odsKafkaDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String jsonStr) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                // 如果不是transport_task表数据，直接丢弃
                if(!"transport_task".equals(jsonObj.getJSONObject("source").getString("table"))){
                    return false;
                }

                JSONObject beforeJsonObj = jsonObj.getJSONObject("before");
                // 如果是before为空，直接丢弃
                if(beforeJsonObj == null){
                    return false;
                }

                String op = jsonObj.getString("op");
                String beforeActualEndTime = beforeJsonObj.getString("actual_end_time");
                String afterActualEndTime = jsonObj.getJSONObject("after").getString("actual_end_time");
                return "u".equals(op) && beforeActualEndTime == null && afterActualEndTime != null;
            }
        });

        // TODO 6、对过滤出的数据进行处理
        SingleOutputStreamOperator<String> transFinishDS = transportTaskDS.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                DwdTransTransFinishBean dwdTransTransFinishBean = jsonObj.getObject("after", DwdTransTransFinishBean.class);

                // 脱敏
                String driver1Name = dwdTransTransFinishBean.getDriver1Name();
                String driver2Name = dwdTransTransFinishBean.getDriver2Name();
                String truckNo = dwdTransTransFinishBean.getTruckNo();

                driver1Name = driver1Name.charAt(0) +
                        driver1Name.substring(1).replaceAll(".", "\\*");
                driver2Name = driver2Name == null ? driver2Name : driver2Name.charAt(0) +
                        driver2Name.substring(1).replaceAll(".", "\\*");
                truckNo = DigestUtils.md5Hex(truckNo);

                dwdTransTransFinishBean.setDriver1Name(driver1Name);
                dwdTransTransFinishBean.setDriver2Name(driver2Name);
                dwdTransTransFinishBean.setTruckNo(truckNo);

                // 补全时间字段
                long actualStartTime = Long.parseLong(dwdTransTransFinishBean.getActualStartTime()) - 8 * 60 * 60 * 1000;
                long actualEndTime = Long.parseLong(dwdTransTransFinishBean.getActualEndTime()) - 8 * 60 * 60 * 1000;
                dwdTransTransFinishBean.setTransportTime(actualEndTime - actualStartTime);
                dwdTransTransFinishBean.setActualStartTime(DateFormatUtil.toYmdHms(actualStartTime));
                dwdTransTransFinishBean.setActualEndTime(DateFormatUtil.toYmdHms(actualEndTime));

                // 补充时间戳
                dwdTransTransFinishBean.setTs(actualEndTime);

                out.collect(JSON.toJSONString(dwdTransTransFinishBean));
            }
        }).uid("process_data");

        // TODO 7、写出到对应Kafka主题
        String transFinishTopic = "tms_dwd_trans_trans_finish";
        KafkaSink<String> kafkaSink = KafkaUtil.createKafkaSink(args, transFinishTopic, transFinishTopic + "_trans");
        transFinishDS.print(">>>");
        transFinishDS.sinkTo(kafkaSink).uid("trans_finish_sink");

        // TODO 8、执行Flink程序
        env.execute();
    }
}
