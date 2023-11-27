package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTransTransFinishBean;
import com.atguigu.tms.realtime.beans.DwsTransOrgTruckModelTransFinishDayBean;
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
 * @create 2023-10-12 22:54
 */
public class DwsTransOrgTruckModelTransFinishDayApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.环境准备
        // 1.1 准备流处理环境 设置检查点
        // 1.2 设置并行度
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        env.setParallelism(4);

        // TODO 2.读取kafka主题数据
        // 2.1 声明消费主题
        String topic = "tms_dwd_trans_trans_finish";
        // 2.2 声明消费者id
        String groupId = "tms_dwd_trans_trans_finish_group";
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("tms_dwd_trans_trans_finish_kafka_source");

        // TODO 3.转换读取的数据jsonStr 为 专用结构，将ts + 8小时
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> mappedDS = kafkaStrDS.map(new MapFunction<String, DwsTransOrgTruckModelTransFinishDayBean>() {
            @Override
            public DwsTransOrgTruckModelTransFinishDayBean map(String jsonStr) throws Exception {
                DwdTransTransFinishBean dwdTransTransFinishBean = JSONObject.parseObject(jsonStr, DwdTransTransFinishBean.class);
                DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean = DwsTransOrgTruckModelTransFinishDayBean.builder()
                        .orgId(dwdTransTransFinishBean.getStartOrgId())
                        .orgName(dwdTransTransFinishBean.getStartOrgName())
                        .truckId(dwdTransTransFinishBean.getTruckId())
                        .transFinishCountBase(1L)
                        .transFinishDurTimeBase(dwdTransTransFinishBean.getTransportTime())
                        .transFinishDistanceBase(dwdTransTransFinishBean.getActualDistance())
                        .ts(dwdTransTransFinishBean.getTs() + 8 * 60 * 60 * 1000)
                        .build();
                return dwsTransOrgTruckModelTransFinishDayBean;
            }
        });

        // TODO 4.关联卡车类型维度，便于后续keyBy
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> joinTruckModelIdDS = AsyncDataStream.unorderedWait(
                mappedDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_truck_info", true) {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean, JSONObject dimJsonObj) {
                        String truckModelId = dimJsonObj.getString("truck_model_id");
                        dwsTransOrgTruckModelTransFinishDayBean.setTruckModelId(truckModelId);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean) {
                        return Tuple2.of("id", dwsTransOrgTruckModelTransFinishDayBean.getTruckId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 5.分配水位线 + 抽取ts
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withWatermarkDS = joinTruckModelIdDS.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTransOrgTruckModelTransFinishDayBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransOrgTruckModelTransFinishDayBean>() {
                    @Override
                    public long extractTimestamp(DwsTransOrgTruckModelTransFinishDayBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        // TODO 6.根据orgId和卡车类型进行keyBy
        KeyedStream<DwsTransOrgTruckModelTransFinishDayBean, String> keyedDS = withWatermarkDS.keyBy(new KeySelector<DwsTransOrgTruckModelTransFinishDayBean, String>() {
            @Override
            public String getKey(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean) throws Exception {
                return dwsTransOrgTruckModelTransFinishDayBean.getOrgId() + ":" + dwsTransOrgTruckModelTransFinishDayBean.getTruckModelId();
            }
        });

        // TODO 7.开窗
        WindowedStream<DwsTransOrgTruckModelTransFinishDayBean, String, TimeWindow> withWindowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.days(1)));

        // TODO 8.关联自定义trigger
        WindowedStream<DwsTransOrgTruckModelTransFinishDayBean, String, TimeWindow> withTirrggerDS = withWindowDS.trigger(new MyTriggerFunction<>());

        // TODO 9.聚合
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> aggregateDS = withTirrggerDS.aggregate(
                new MyAggregationFunction<DwsTransOrgTruckModelTransFinishDayBean>() {
                    @Override
                    public DwsTransOrgTruckModelTransFinishDayBean add(DwsTransOrgTruckModelTransFinishDayBean value, DwsTransOrgTruckModelTransFinishDayBean accumulator) {
                        if(accumulator == null){
                            return value;
                        }

                        accumulator.setTransFinishCountBase(value.getTransFinishCountBase() + accumulator.getTransFinishCountBase());
                        accumulator.setTransFinishDistanceBase(value.getTransFinishDistanceBase().add(accumulator.getTransFinishDistanceBase()));
                        accumulator.setTransFinishDurTimeBase(value.getTransFinishDurTimeBase() + accumulator.getTransFinishDurTimeBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTransOrgTruckModelTransFinishDayBean, DwsTransOrgTruckModelTransFinishDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTransOrgTruckModelTransFinishDayBean> elements, Collector<DwsTransOrgTruckModelTransFinishDayBean> out) throws Exception {
                        long start = context.window().getStart();
                        for (DwsTransOrgTruckModelTransFinishDayBean element : elements) {
                            element.setCurDate(DateFormatUtil.toYmdHms(start - 8 * 60 * 60 * 1000));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 10.维度关联
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> joinModelNameDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_truck_model",true) {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean, JSONObject dimJsonObj) {
                        String modelName = dimJsonObj.getString("model_name");
                        dwsTransOrgTruckModelTransFinishDayBean.setTruckModelName(modelName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean) {
                        return Tuple2.of("id",dwsTransOrgTruckModelTransFinishDayBean.getTruckModelId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> joinOrgParentIdDS = AsyncDataStream.unorderedWait(
                joinModelNameDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_base_organ", true) {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean, JSONObject dimJsonObj) {
                        String orgParentId = dimJsonObj.getString("org_parent_id");
                        String joinOrgId = orgParentId == null? dwsTransOrgTruckModelTransFinishDayBean.getOrgId() : orgParentId;
                        dwsTransOrgTruckModelTransFinishDayBean.setJoinOrgId(joinOrgId);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean) {
                        return Tuple2.of("id",dwsTransOrgTruckModelTransFinishDayBean.getOrgId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );


        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> joinCityIdDS = AsyncDataStream.unorderedWait(
                joinOrgParentIdDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_base_organ", true) {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean, JSONObject dimJsonObj) {
                        String cityId = dimJsonObj.getString("region_id");
                        dwsTransOrgTruckModelTransFinishDayBean.setCityId(cityId);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean) {
                        return Tuple2.of("id",dwsTransOrgTruckModelTransFinishDayBean.getJoinOrgId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> joinCityNameDS = AsyncDataStream.unorderedWait(
                joinCityIdDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_base_region_info", true) {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean, JSONObject dimJsonObj) {
                        String cityName = dimJsonObj.getString("name");
                        dwsTransOrgTruckModelTransFinishDayBean.setCityName(cityName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean dwsTransOrgTruckModelTransFinishDayBean) {
                        return Tuple2.of("id",dwsTransOrgTruckModelTransFinishDayBean.getCityId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        joinCityNameDS.print(">>>");

        // TODO 11.写入clickhouse
        String sql = "insert into table dws_trans_org_truck_model_trans_finish_day_base values (?,?,?,?,?,?,?,?,?,?,?)";
        joinCityNameDS.addSink(ClickHouseUtil.getJdbcSink(sql)).uid("dws_trans_org_truck_model_trans_finish_day_base_sink");

        // TODO 12.执行flink
        env.execute();
    }
}
