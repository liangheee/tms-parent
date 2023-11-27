package com.atguigu.tms.realtime.app.ods;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Hliang
 * @create 2023-09-23 17:50
        */
public class OdsApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建环境对象
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        // TODO 2、设置并行度
        env.setParallelism(4);

        // TODO 3、读取mysql数据，写入到kafka
        String dwdOption = "dwd";
        String dwdSourceName = "dwdMysqlSource";
        // flink-cdc的MysqlSource中指定serverId范围在5400~6400
        String dwdServerId = "6030";
        mysqlToKafka(args,env,dwdOption,dwdSourceName,dwdServerId);

        String realtimeDimOption = "realtime-dim";
        String realtimeDimSourceName = "realtimeDimMysqlSource";
        // flink-cdc的MysqlSource中指定serverId范围在5400~6400
        String realtimeDimServerId = "6040";
        mysqlToKafka(args,env,realtimeDimOption,realtimeDimSourceName,realtimeDimServerId);

        // TODO 4、开启执行flink-cdc
        env.execute();
    }

    /**
     * 增量同步MySQL数据到Kafka
     * @param args 动态配置参数
     * @param env flink环境对象StreamExecutionEnvironment
     * @param option 取值dwd | realtime-dim （选择读取dwd层数据还是dim层数据）
     * @param sourceName 配置MySQL Source名称
     * @param serverId flink-cdc模拟mysql主从复制中的从机，serverId的范围为5400 ~ 6400
     */
    public static void mysqlToKafka(String[] args,StreamExecutionEnvironment env,String option,String sourceName,String serverId){
        // 读取MySQL数据
        MySqlSource<String> mysqlSource = CreateEnvUtil.createMysqlSource(args, option, serverId);
        // 为避免乱序，设置并行度为1
        SingleOutputStreamOperator<String> strDs = env.
                fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), sourceName)
                .setParallelism(1)
                .uid(option + "_"  + "source");

        // 进行ETL
        // 1、过滤掉delete操作 2、将其中的ts_ms替换为ts
        // 为避免乱序，设置并行度为1
        SingleOutputStreamOperator<String> processDS = strDs.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                if (jsonObject.getJSONObject("after") != null && !"d".equals(jsonObject.getString("op"))) {
                    // 如果当前存在after并且不是删除操作，继续修改ts_ms为ts
                    String tsMs = jsonObject.getString("ts_ms");
                    jsonObject.put("ts", tsMs);
                    jsonObject.remove("ts_ms");
                    out.collect(JSONObject.toJSONString(jsonObject));
                }
            }
        }).setParallelism(1)
                .uid(option + "_"  + "process");

        // TODO !!!!为方便后面DIM、DWD的处理，这里一定要按照主键分组，使得相同组的数据去往同一个分区（这样HBase才能拿到最新数据）
        KeyedStream<String, String> keyedDS = processDS.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                JSONObject after = jsonObject.getJSONObject("after");
                String id = after.getString("id");
                return id;
            }
        });

        // 写入Kafka的tms01_ods主题
        KafkaSink<String> kafkaSink = KafkaUtil.createKafkaSink(args, "tms01_ods", option + "_" + "trans");

        keyedDS.sinkTo(kafkaSink).uid(option + "_"  + "sink");
    }
}
