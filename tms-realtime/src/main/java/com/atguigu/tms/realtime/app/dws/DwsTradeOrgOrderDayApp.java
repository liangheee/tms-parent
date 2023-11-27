package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTradeOrderDetailBean;
import com.atguigu.tms.realtime.beans.DwsTradeOrgOrderDayBean;
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
 * @create 2023-10-12 19:44
 */
public class DwsTradeOrgOrderDayApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、环境准备
        // 1.1 准备流处理环境、设置检查点
        // 1.2 设置并行度
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        env.setParallelism(4);

        // TODO 2、读取kafka主题数据
        // 2.1 声明消费主题
        String topic = "tms_dwd_trade_order_detail";
        // 2.2 声明消费者组id
        String groupId = "dws_trade_org_order_day_group";
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("dws_trade_org_order_day_kafka_source");

        // TODO 3、将读取到的jsonStr转换为专用结构，ts + 8小时
        // 3.1 将jsonStr转换为专用结构
        // 3.2 将ts + 8小时
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> mappedDS = kafkaStrDS.map(new MapFunction<String, DwsTradeOrgOrderDayBean>() {
            @Override
            public DwsTradeOrgOrderDayBean map(String jsonStr) throws Exception {
                DwdTradeOrderDetailBean dwdTradeOrderDetailBean = JSON.parseObject(jsonStr, DwdTradeOrderDetailBean.class);
                DwsTradeOrgOrderDayBean dwsTradeOrgOrderDayBean = DwsTradeOrgOrderDayBean.builder()
                        .cityId(dwdTradeOrderDetailBean.getSenderCityId())
                        .senderDistrictId(dwdTradeOrderDetailBean.getSenderDistrictId())
                        .orderAmountBase(dwdTradeOrderDetailBean.getAmount())
                        .orderCountBase(1L)
                        .ts(dwdTradeOrderDetailBean.getTs() + 8 * 60 * 60 * 1000)
                        .build();
                return dwsTradeOrgOrderDayBean;
            }
        });

        // TODO 4、维度关联获取orgId和orgName
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withOrgIdAndOrgNameDS = AsyncDataStream.unorderedWait(
                mappedDS,
                new DimAsyncFunction<DwsTradeOrgOrderDayBean>("dim_base_organ",false) {
                    @Override
                    public void join(DwsTradeOrgOrderDayBean dwsTradeOrgOrderDayBean, JSONObject dimJsonObj) {
                        String orgId = dimJsonObj.getString("id");
                        String orgName = dimJsonObj.getString("org_name");
                        dwsTradeOrgOrderDayBean.setOrgId(orgId);
                        dwsTradeOrgOrderDayBean.setOrgName(orgName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTradeOrgOrderDayBean dwsTradeOrgOrderDayBean) {
                        return Tuple2.of("region_id",dwsTradeOrgOrderDayBean.getSenderDistrictId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );


        // TODO 5、分配水位线，指定抽取ts
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withWatermarkDS = withOrgIdAndOrgNameDS.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTradeOrgOrderDayBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeOrgOrderDayBean>() {
                    @Override
                    public long extractTimestamp(DwsTradeOrgOrderDayBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        // TODO 6、按照orgId分组
        KeyedStream<DwsTradeOrgOrderDayBean, String> keyedDS = withWatermarkDS.keyBy(new KeySelector<DwsTradeOrgOrderDayBean, String>() {
            @Override
            public String getKey(DwsTradeOrgOrderDayBean dwsTradeOrgOrderDayBean) throws Exception {
                return dwsTradeOrgOrderDayBean.getOrgId();
            }
        });

        // TODO 7、开窗
        WindowedStream<DwsTradeOrgOrderDayBean, String, TimeWindow> withWindowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.days(1)));

        // TODO 8、关联触发器
        WindowedStream<DwsTradeOrgOrderDayBean, String, TimeWindow> withTriggerDS = withWindowDS.trigger(new MyTriggerFunction<>());

        // TODO 8、聚合
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> aggregateDS = withTriggerDS.aggregate(
                new MyAggregationFunction<DwsTradeOrgOrderDayBean>() {
                    @Override
                    public DwsTradeOrgOrderDayBean add(DwsTradeOrgOrderDayBean dwsTradeOrgOrderDayBean, DwsTradeOrgOrderDayBean accumulator) {
                        if (accumulator == null) {
                            return dwsTradeOrgOrderDayBean;
                        }
                        accumulator.setOrderAmountBase(dwsTradeOrgOrderDayBean.getOrderAmountBase().add(accumulator.getOrderAmountBase()));
                        accumulator.setOrderCountBase(dwsTradeOrgOrderDayBean.getOrderCountBase() + accumulator.getOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTradeOrgOrderDayBean, DwsTradeOrgOrderDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTradeOrgOrderDayBean> elements, Collector<DwsTradeOrgOrderDayBean> out) throws Exception {
                        long start = context.window().getStart();
                        for (DwsTradeOrgOrderDayBean element : elements) {
                            element.setCurDate(DateFormatUtil.toYmdHms(start - 8 * 60 * 60 * 1000));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 9、维度关联
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withCityNameDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsTradeOrgOrderDayBean>("dim_base_region_info", true) {
                    @Override
                    public void join(DwsTradeOrgOrderDayBean dwsTradeOrgOrderDayBean, JSONObject dimJsonObj) {
                        String cityName = dimJsonObj.getString("name");
                        dwsTradeOrgOrderDayBean.setCityName(cityName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTradeOrgOrderDayBean dwsTradeOrgOrderDayBean) {
                        return Tuple2.of("id", dwsTradeOrgOrderDayBean.getCityId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        withCityNameDS.print(">>>");

        // TODO 10、写出到clickhouse
        String sql = "insert into table dws_trade_org_order_day_base values (?,?,?,?,?,?,?,?)";
        withCityNameDS.addSink(ClickHouseUtil.getJdbcSink(sql)).uid("dws_trade_org_order_day_sink");

        // TODO 11、执行flink
        env.execute();
    }
}
