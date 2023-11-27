package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTransDeliverSucDetailBean;
import com.atguigu.tms.realtime.beans.DwsTransOrgDeliverSucDayBean;
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
 * @create 2023-10-12 21:53
 */
public class DwsTransOrgDeliverSucDayApp {
    public static void main(String[] args) throws Exception {
        // TODO 1、环境准备
        // 1.1 准备流处理环境 设置检查点
        // 1.2 设置并行度
        StreamExecutionEnvironment env = CreateEnvUtil.createStreamEnv(args);
        env.setParallelism(4);
        // TODO 2、读取kafka主题数据
        // 2.1 声明消费者主题
        String topic = "tms_dwd_trans_deliver_detail";
        // 2.2 声明消费者id
        String groupId = "tms_dwd_trans_deliver_detail_group";
        KafkaSource<String> kafkaSource = KafkaUtil.createKafkaSource(args, topic, groupId);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("tms_dwd_trans_deliver_detail_kafka_source");

        // TODO 3、转换读取数据jsonStr 为 专用结构，将ts + 8小时
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> mappedDS = kafkaStrDS.map(new MapFunction<String, DwsTransOrgDeliverSucDayBean>() {
            @Override
            public DwsTransOrgDeliverSucDayBean map(String jsonStr) throws Exception {
                DwdTransDeliverSucDetailBean dwdTransDeliverSucDetailBean = JSONObject.parseObject(jsonStr, DwdTransDeliverSucDetailBean.class);
                DwsTransOrgDeliverSucDayBean dwsTransOrgDeliverSucDayBean = DwsTransOrgDeliverSucDayBean.builder()
                        .cityId(dwdTransDeliverSucDetailBean.getReceiverCityId())
                        .provinceId(dwdTransDeliverSucDetailBean.getReceiverProvinceId())
                        .districtId(dwdTransDeliverSucDetailBean.getReceiverDistrictId())
                        .deliverSucCountBase(1L)
                        .ts(dwdTransDeliverSucDetailBean.getTs() + 8 * 60 * 60 * 1000)
                        .build();
                return dwsTransOrgDeliverSucDayBean;
            }
        });

        // TODO 4、关联orgId和orgName，用于后续keyBy
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withOrgIdAndOrgNameDS = AsyncDataStream.unorderedWait(
                mappedDS,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_organ",false) {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean dwsTransOrgDeliverSucDayBean, JSONObject dimJsonObj) {
                        String orgId = dimJsonObj.getString("id");
                        String orgName = dimJsonObj.getString("org_name");
                        dwsTransOrgDeliverSucDayBean.setOrgId(orgId);
                        dwsTransOrgDeliverSucDayBean.setOrgName(orgName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgDeliverSucDayBean dwsTransOrgDeliverSucDayBean) {
                        return Tuple2.of("region_id",dwsTransOrgDeliverSucDayBean.getDistrictId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 5、分配水位线 + 抽取ts
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withWatermarkDS = withOrgIdAndOrgNameDS.assignTimestampsAndWatermarks(WatermarkStrategy.<DwsTransOrgDeliverSucDayBean>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransOrgDeliverSucDayBean>() {
                    @Override
                    public long extractTimestamp(DwsTransOrgDeliverSucDayBean element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        // TODO 6、按照orgId分组
        KeyedStream<DwsTransOrgDeliverSucDayBean, String> keyedDS = withWatermarkDS.keyBy(new KeySelector<DwsTransOrgDeliverSucDayBean, String>() {
            @Override
            public String getKey(DwsTransOrgDeliverSucDayBean value) throws Exception {
                return value.getOrgId();
            }
        });

        // TODO 7、开窗
        WindowedStream<DwsTransOrgDeliverSucDayBean, String, TimeWindow> withWindowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.days(1)));

        // TODO 8、关联触发器
        WindowedStream<DwsTransOrgDeliverSucDayBean, String, TimeWindow> withTriggerDS = withWindowDS.trigger(new MyTriggerFunction<>());

        // TODO 9、聚合
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> aggregateDS = withTriggerDS.aggregate(
                new MyAggregationFunction<DwsTransOrgDeliverSucDayBean>() {
                    @Override
                    public DwsTransOrgDeliverSucDayBean add(DwsTransOrgDeliverSucDayBean dwsTransOrgDeliverSucDayBean, DwsTransOrgDeliverSucDayBean accumulator) {
                        if(accumulator == null){
                            return dwsTransOrgDeliverSucDayBean;
                        }
                        accumulator.setDeliverSucCountBase(dwsTransOrgDeliverSucDayBean.getDeliverSucCountBase() + accumulator.getDeliverSucCountBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTransOrgDeliverSucDayBean, DwsTransOrgDeliverSucDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTransOrgDeliverSucDayBean> elements, Collector<DwsTransOrgDeliverSucDayBean> out) throws Exception {
                        long start = context.window().getStart();
                        for (DwsTransOrgDeliverSucDayBean element : elements) {
                            element.setCurDate(DateFormatUtil.toYmdHms(start - 8 * 60 * 60 * 1000));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );

        // TODO 10、关联维度
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> joinCityNameDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_region_info",true) {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean dwsTransOrgDeliverSucDayBean, JSONObject dimJsonObj) {
                        String cityName = dimJsonObj.getString("name");
                        dwsTransOrgDeliverSucDayBean.setCityName(cityName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgDeliverSucDayBean dwsTransOrgDeliverSucDayBean) {
                        return Tuple2.of("id",dwsTransOrgDeliverSucDayBean.getCityId());
                    }
                },
                60,
                TimeUnit.SECONDS

        );

        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> joinProvinceNameDS = AsyncDataStream.unorderedWait(
                joinCityNameDS,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_region_info", true) {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean dwsTransOrgDeliverSucDayBean, JSONObject dimJsonObj) {
                        String provinceName = dimJsonObj.getString("name");
                        dwsTransOrgDeliverSucDayBean.setProvinceName(provinceName);
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgDeliverSucDayBean dwsTransOrgDeliverSucDayBean) {
                        return Tuple2.of("id",dwsTransOrgDeliverSucDayBean.getProvinceId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        joinProvinceNameDS.print(">>>");

        // TODO 11、写入clickhouse
        String sql = "insert into table dws_trans_org_deliver_suc_day_base values (?,?,?,?,?,?,?,?,?)";
        joinProvinceNameDS.addSink(ClickHouseUtil.getJdbcSink(sql)).uid("dws_trans_org_deliver_suc_day_sink");

        // TODO 12、执行flink
        env.execute();
    }
}
