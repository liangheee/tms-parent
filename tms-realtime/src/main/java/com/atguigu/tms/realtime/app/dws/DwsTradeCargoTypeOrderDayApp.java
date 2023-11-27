package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTradeOrderDetailBean;
import com.atguigu.tms.realtime.beans.DwsTradeCargoTypeOrderDayBean;
import com.atguigu.tms.realtime.utils.ClickHouseUtil;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author Hliang
 * @create 2023-10-11 23:01
 */
public class DwsTradeCargoTypeOrderDayApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、环境准备
        // 1.1、创建流处理环节，设置检查点
        // 1.2、设置并行度
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        env.setParallelism(4);
        // TODO 2、读取kafka主题数据
        // 2.1 声明消费主题
        String topic = "tms_dwd_trade_order_detail";
        // 2.2 声明消费者组id
        String groupId = "dws_trade_cargo_type_order_day_group";
        // 2.3 消费kafka数据
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("dws_trade_cargo_type_order_day_kafka_source");

        // TODO 3、将读取数据jsonStr 转换为 专用结构
        // 3.1 转换为专用结构
        // 3.2 ts + 8小时
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> mappedDS = kafkaStrDS.map(new MapFunction<String, DwsTradeCargoTypeOrderDayBean>() {
            @Override
            public DwsTradeCargoTypeOrderDayBean map(String jsonStr) throws Exception {
                DwdTradeOrderDetailBean dwdTradeOrderDetailBean = JSONObject.parseObject(jsonStr, DwdTradeOrderDetailBean.class);
                DwsTradeCargoTypeOrderDayBean dwsTradeCargoTypeOrderDayBean = DwsTradeCargoTypeOrderDayBean.builder()
                        .cargoType(dwdTradeOrderDetailBean.getCargoType())
                        .orderAmountBase(dwdTradeOrderDetailBean.getAmount())
                        .orderCountBase(1L)
                        .ts(dwdTradeOrderDetailBean.getTs() + 8 * 60 * 60 * 1000)
                        .build();
                return dwsTradeCargoTypeOrderDayBean;
            }
        });

        // TODO 4、分配水位线，抽取Ts
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> withWatermarkDS = mappedDS.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradeCargoTypeOrderDayBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeCargoTypeOrderDayBean>() {
                    @Override
                    public long extractTimestamp(DwsTradeCargoTypeOrderDayBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        // TODO 5、按照cargoType进行keyBy
        KeyedStream<DwsTradeCargoTypeOrderDayBean, String> keyedDS = withWatermarkDS.keyBy(new KeySelector<DwsTradeCargoTypeOrderDayBean, String>() {
            @Override
            public String getKey(DwsTradeCargoTypeOrderDayBean value) throws Exception {
                return value.getCargoType();
            }
        });

        // TODO 6、开窗
        WindowedStream<DwsTradeCargoTypeOrderDayBean, String, TimeWindow> withWindowDS = keyedDS
                .window(TumblingEventTimeWindows.of(Time.days(1)));

        // TODO 7、关联自定义触发器
        WindowedStream<DwsTradeCargoTypeOrderDayBean, String, TimeWindow> withTriggerDS = withWindowDS
                .trigger(new MyTriggerFunction<>());

        // TODO 8、聚合
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> aggregateDS = withTriggerDS.aggregate(
                new MyAggregationFunction<DwsTradeCargoTypeOrderDayBean>() {
                    @Override
                    public DwsTradeCargoTypeOrderDayBean add(DwsTradeCargoTypeOrderDayBean value, DwsTradeCargoTypeOrderDayBean accumulator) {
                        if(accumulator == null){
                            return value;
                        }
                        accumulator.setOrderAmountBase(value.getOrderAmountBase().add(accumulator.getOrderAmountBase()));
                        accumulator.setOrderCountBase(value.getOrderCountBase() + accumulator.getOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTradeCargoTypeOrderDayBean, DwsTradeCargoTypeOrderDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTradeCargoTypeOrderDayBean> elements, Collector<DwsTradeCargoTypeOrderDayBean> out) throws Exception {
                        long start = context.window().getStart();
                        for (DwsTradeCargoTypeOrderDayBean element : elements) {
                            element.setCurDate(DateFormatUtil.toYmdHms(start - 8 * 60 * 60 * 1000));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 9、维度关联
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> withDimDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsTradeCargoTypeOrderDayBean>("dim_base_dic",true) {
                    @Override
                    public void join(DwsTradeCargoTypeOrderDayBean dwsTradeCargoTypeOrderDayBean, JSONObject dimJsonObj) {
                        String name = dimJsonObj.getString("name");
                        dwsTradeCargoTypeOrderDayBean.setCargoTypeName(name);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTradeCargoTypeOrderDayBean dwsTradeCargoTypeOrderDayBean) {
                        return Tuple2.of("id",dwsTradeCargoTypeOrderDayBean.getCargoType());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        withDimDS.print(">>>");

        // TODO 10、写入Clickhouse
        String sql = "insert into table dws_trade_cargo_type_order_day_base values(?,?,?,?,?,?)";
        withDimDS.addSink(ClickHouseUtil.getJdbcSink(sql)).uid("dws_trade_cargo_type_order_day_sink");

        // TODO 11、执行flink
        env.execute();
    }

}
