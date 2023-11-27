package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTransReceiveDetailBean;
import com.atguigu.tms.realtime.beans.DwsTransOrgReceiveDayBean;
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
 * @create 2023-10-12 22:33
 */
public class DwsTransOrgReceiveDayApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.环境准备
        // 1.1 准备流处理环境 设置检查点
        // 1.2 设置并行度
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        env.setParallelism(4);

        // TODO 2.读取kafka主题数据
        // 2.1 声明消费的主题
        String topic = "tms_dwd_trans_receive_detail";
        // 2.2 声明消费者id
        String groupId = "tms_dwd_trans_receive_detail_group";
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("tms_dwd_trans_receive_detail_kafka_source");

        // TODO 3.转换读取数据 jsonStr 转换为 专用结构，将ts + 8小时
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> mappedDS = kafkaStrDS.map(new MapFunction<String, DwsTransOrgReceiveDayBean>() {
            @Override
            public DwsTransOrgReceiveDayBean map(String jsonStr) throws Exception {
                DwdTransReceiveDetailBean dwdTransReceiveDetailBean = JSONObject.parseObject(jsonStr, DwdTransReceiveDetailBean.class);
                DwsTransOrgReceiveDayBean dwsTransOrgReceiveDayBean = DwsTransOrgReceiveDayBean.builder()
                        .cityId(dwdTransReceiveDetailBean.getSenderCityId())
                        .provinceId(dwdTransReceiveDetailBean.getSenderProvinceId())
                        .districtId(dwdTransReceiveDetailBean.getSenderDistrictId())
                        .receiveOrderCountBase(1L)
                        .ts(dwdTransReceiveDetailBean.getTs() + 8 * 60 * 60 * 1000)
                        .build();
                return dwsTransOrgReceiveDayBean;
            }
        });

        // TODO 4.关联维度，获取orgId和orgName
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> withOrgIdAndOrgNameDS = AsyncDataStream.unorderedWait(
                mappedDS,
                new DimAsyncFunction<DwsTransOrgReceiveDayBean>("dim_base_organ", false) {
                    @Override
                    public void join(DwsTransOrgReceiveDayBean dwsTransOrgReceiveDayBean, JSONObject dimJsonObj) {
                        String orgId = dimJsonObj.getString("id");
                        String orgName = dimJsonObj.getString("org_name");
                        dwsTransOrgReceiveDayBean.setOrgId(orgId);
                        dwsTransOrgReceiveDayBean.setOrgName(orgName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgReceiveDayBean dwsTransOrgReceiveDayBean) {
                        return Tuple2.of("region_id",dwsTransOrgReceiveDayBean.getDistrictId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 5.分配水位线 + 抽取ts
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> withWatermarkDS = withOrgIdAndOrgNameDS.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTransOrgReceiveDayBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransOrgReceiveDayBean>() {
                    @Override
                    public long extractTimestamp(DwsTransOrgReceiveDayBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        // TODO 6.通过orgId进行keyBy
        KeyedStream<DwsTransOrgReceiveDayBean, String> keyedDS = withWatermarkDS.keyBy(new KeySelector<DwsTransOrgReceiveDayBean, String>() {
            @Override
            public String getKey(DwsTransOrgReceiveDayBean value) throws Exception {
                return value.getOrgId();
            }
        });

        // TODO 7.开窗
        WindowedStream<DwsTransOrgReceiveDayBean, String, TimeWindow> withWindowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.days(1)));

        // TODO 8、关联自定义trigger
        WindowedStream<DwsTransOrgReceiveDayBean, String, TimeWindow> withTriggerDS = withWindowDS.trigger(new MyTriggerFunction<>());

        // TODO 9.聚合
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> aggregateDS = withTriggerDS.aggregate(
                new MyAggregationFunction<DwsTransOrgReceiveDayBean>() {
                    @Override
                    public DwsTransOrgReceiveDayBean add(DwsTransOrgReceiveDayBean dwsTransOrgReceiveDayBean, DwsTransOrgReceiveDayBean accumulator) {
                        if(accumulator == null){
                            return dwsTransOrgReceiveDayBean;
                        }
                        accumulator.setReceiveOrderCountBase(dwsTransOrgReceiveDayBean.getReceiveOrderCountBase() + accumulator.getReceiveOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTransOrgReceiveDayBean, DwsTransOrgReceiveDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTransOrgReceiveDayBean> elements, Collector<DwsTransOrgReceiveDayBean> out) throws Exception {
                        long start = context.window().getStart();
                        for (DwsTransOrgReceiveDayBean element : elements) {
                            element.setCurDate(DateFormatUtil.toYmdHms(start - 8 * 60 * 60 * 1000));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 10.关联维度
        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> joinCityNameDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsTransOrgReceiveDayBean>("dim_base_region_info", true) {
                    @Override
                    public void join(DwsTransOrgReceiveDayBean dwsTransOrgReceiveDayBean, JSONObject dimJsonObj) {
                        String cityName = dimJsonObj.getString("name");
                        dwsTransOrgReceiveDayBean.setCityName(cityName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgReceiveDayBean dwsTransOrgReceiveDayBean) {
                        return Tuple2.of("id",dwsTransOrgReceiveDayBean.getCityId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<DwsTransOrgReceiveDayBean> joinProvinceNameDS = AsyncDataStream.unorderedWait(
                joinCityNameDS,
                new DimAsyncFunction<DwsTransOrgReceiveDayBean>("dim_base_region_info", true) {
                    @Override
                    public void join(DwsTransOrgReceiveDayBean dwsTransOrgReceiveDayBean, JSONObject dimJsonObj) {
                        String provinceName = dimJsonObj.getString("name");
                        dwsTransOrgReceiveDayBean.setProvinceName(provinceName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgReceiveDayBean dwsTransOrgReceiveDayBean) {
                        return Tuple2.of("id",dwsTransOrgReceiveDayBean.getProvinceId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        joinProvinceNameDS.print(">>>");

        // TODO 11.写入clickhouse
        String sql = "insert into table dws_trans_org_receive_day_base values (?,?,?,?,?,?,?,?,?)";
        joinProvinceNameDS.addSink(ClickHouseUtil.getJdbcSink(sql)).uid("dws_trans_org_receive_day_base_sink");

        // TODO 12.执行flink
        env.execute();
    }
}
