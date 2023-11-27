package com.atguigu.tms.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimSinkFunction;
import com.atguigu.tms.realtime.app.func.MyBroadcastProcessFunction;
import com.atguigu.tms.realtime.beans.TmsConfigDimBean;
import com.atguigu.tms.realtime.common.TmsConfig;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.HBaseUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hliang
 * @create 2023-09-24 18:48
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、配置环境对象
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);

        // TODO 2、配置并行度
        env.setParallelism(4);

        // TODO 3、通过kafka从tms01_ods主题读取主流数据
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, "tms01_ods", "dim_consumer_group");
        SingleOutputStreamOperator<String> odsStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
                .uid("kafka_ods_source");


        // TODO 4、对读取的主流数据进行ETL
        //      清除before、transaction；
        //      将source中的table字段抽取出来封装进json字符串，然后清除source字段
        SingleOutputStreamOperator<JSONObject> odsStrMappedDS = odsStrDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String table =  jsonObject.getJSONObject("source").getString("table");
                jsonObject.put("table", table);
                jsonObject.remove("transaction");
                jsonObject.remove("source");
                return jsonObject;
            }
        });

        // TODO 5、通过Flink-CDC读取MySQL中配置的业务维度表和HBase维度表的映射
        MySqlSource<String> mysqlSource = CreateEnvUtil.createMysqlSource(args, "tms_config_dim", "6050");
        SingleOutputStreamOperator<String> rawTmsConfigDimDS = env
                .fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "tmsConfigDim_MysqlSource")
                .setParallelism(1)
                .uid("tmsConfigDim_source");

        // TODO 6、通过对应的配置流数据在HBase中创建对应的表
        SingleOutputStreamOperator<String> tmsConfigDimDS = rawTmsConfigDimDS.map(new MapFunction<String, String>() {
            @Override
            public String map(String jsonStr) throws Exception {
                // 我们只用对 op为r、c的操作创建表
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                String op = jsonObject.getString("op");
                if ("r".equals(op) || "c".equals(op)) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    String sinkTable = after.getString("sink_table");
                    String sinkFamily = after.getString("sink_family");
                    if(StringUtils.isEmpty(sinkFamily)){
                        sinkFamily = "info";
                    }
                    System.out.println("在HBase中创建表：" + sinkTable);
                    HBaseUtil.createTable(TmsConfig.HBASE_NAMESPACE, sinkTable, sinkFamily.split(","));
                }
                return jsonStr;
            }
        });

        // TODO 7、将获取的表的映射广播，存储dim维度表状态
        //  广播配置流
        MapStateDescriptor<String, TmsConfigDimBean> tmsConfigDimBeanMapStateDescriptor = new MapStateDescriptor<>("tmsConfigDimMapState", Types.STRING,
                Types.POJO(TmsConfigDimBean.class));
        BroadcastStream<String> tmsConfigDimBroadcast = tmsConfigDimDS.broadcast(tmsConfigDimBeanMapStateDescriptor);
        //        // 合并主流和广播流
        BroadcastConnectedStream<JSONObject, String> dimConnectedStream = odsStrMappedDS.connect(tmsConfigDimBroadcast);
        // 使用process算子，对主流和广播流数据分别处理
        // TODO 这里process算子并行度为4，但是MyBroadcastProcessFunction中定义了tmsConfigMap，这个也会有4份，还是说只有1份
        SingleOutputStreamOperator<JSONObject> dimBCS = dimConnectedStream.process(new MyBroadcastProcessFunction(tmsConfigDimBeanMapStateDescriptor,args));

        // TODO 8、主流通过dim维度表状态，将业务维度数据刷写到HBase
        dimBCS.print(">>>>");
        dimBCS.addSink(new DimSinkFunction()).uid("dim_sink");

        // TODO 9、执行
        env.execute();
    }
}
